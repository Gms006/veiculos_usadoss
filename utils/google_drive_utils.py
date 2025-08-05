import os
import io
import logging
from typing import Dict, List, Tuple
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
ROOT_FOLDER_ID = "1ADaMbXNPEX8ZIT7c1U_pWMsRygJFROZq"

log = logging.getLogger(__name__)


def get_drive_service() -> "googleapiclient.discovery.Resource":
    """Retorna o serviço do Google Drive utilizando ``GCP_SERVICE_ACCOUNT_JSON``.

    A variável de ambiente deve conter o JSON da chave de serviço. Erros de
    ausência ou formatação incorreta são relatados explicitamente.
    """
    raw_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if not raw_json:
        raise EnvironmentError(
            "Variável GCP_SERVICE_ACCOUNT_JSON não definida"
        )
    try:
        service_account_info = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "Conteúdo inválido em GCP_SERVICE_ACCOUNT_JSON"
        ) from exc

    creds = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def _find_subfolder(service, parent_id: str, name: str) -> str | None:
    """Retorna o ID da subpasta com o nome fornecido."""
    query = (
        f"'{parent_id}' in parents and \
         mimeType='application/vnd.google-apps.folder' and \
         trashed=false"
    )
    results = service.files().list(q=query, fields='files(id, name)').execute()
    for f in results.get('files', []):
        if f['name'].lower() == name.lower():
            return f['id']
    return None


def _list_files(service, folder_id: str) -> List[dict]:
    """Lista todos os arquivos dentro da pasta informada."""
    query = f"'{folder_id}' in parents and trashed=false"
    files: List[dict] = []
    page_token = None
    while True:
        results = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                pageToken=page_token,
                pageSize=1000,
            )
            .execute()
        )
        files.extend(results.get("files", []))
        page_token = results.get("nextPageToken")
        if not page_token:
            break
    return files



def _read_index(service, company_id: str) -> Tuple[Dict[str, Dict], str | None]:
    """Lê o arquivo ``index_arquivos.json`` da empresa."""
    query = (
        f"'{company_id}' in parents and "
        "name='index_arquivos.json' and trashed=false"
    )
    res = service.files().list(q=query, fields="files(id)").execute()
    files = res.get("files")
    if not files:
        return {}, None
    idx_id = files[0]["id"]
    request = service.files().get_media(fileId=idx_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return json.load(buf), idx_id


def _write_index(
    service, company_id: str, index: Dict[str, Dict], file_id: str | None
) -> str:
    """Grava ``index_arquivos.json`` na pasta da empresa."""
    media = MediaIoBaseUpload(
        io.BytesIO(json.dumps(index, ensure_ascii=False, indent=2).encode("utf-8")),
        mimetype="application/json",
        resumable=False,
    )
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute()
        return file_id
    meta = {"name": "index_arquivos.json", "parents": [company_id]}
    result = service.files().create(body=meta, media_body=media).execute()
    return result["id"]


def _infer_tipo_nota(service, file_id: str) -> str:
    """Obtém o campo ``tpNF`` do XML para definir Entrada ou Saída."""
    try:
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buf.seek(0)
        import xml.etree.ElementTree as ET

        tree = ET.parse(buf)
        tp_elem = tree.find(
            ".//{http://www.portalfiscal.inf.br/nfe}tpNF"
        )
        if tp_elem is not None:
            if tp_elem.text == "0":
                return "Entrada"
            if tp_elem.text == "1":
                return "Saída"
    except Exception:
        log.exception("Erro ao inferir tipo de nota")
    return "Indefinido"


def atualizar_index_empresa(service, company_id: str) -> Dict[str, Dict]:
    """Atualiza ou cria o ``index_arquivos.json`` para a empresa."""
    index, idx_id = _read_index(service, company_id)
    arquivos = _scan_xmls(service, company_id)

    atual: Dict[str, Dict] = {}
    for arq in arquivos:
        file_id = arq["id"]
        info = index.get(file_id, {})
        if info.get("modificado") != arq.get("modifiedTime"):
            tipo = info.get("tipo")
            if not tipo or info.get("modificado") != arq.get("modifiedTime"):
                tipo = _infer_tipo_nota(service, file_id)
            atual[file_id] = {
                "nome": arq["name"],
                "caminho": arq["path"],
                "modificado": arq.get("modifiedTime"),
                "tipo": tipo,
            }
        else:
            atual[file_id] = info

    changed = index != atual
    if changed:
        _write_index(service, company_id, atual, idx_id)
    return atual


def _scan_xmls(service, folder_id: str, prefix: str = "") -> List[Dict[str, str]]:
    """Retorna metadados de todos os XMLs abaixo de ``folder_id``."""
    entries: List[Dict[str, str]] = []
    files = _list_files(service, folder_id)
    for f in files:
        if f["mimeType"] == "application/vnd.google-apps.folder":
            new_prefix = os.path.join(prefix, f["name"])
            entries.extend(_scan_xmls(service, f["id"], new_prefix))
            continue
        if f["name"].lower().endswith(".xml"):
            entries.append(
                {
                    "id": f["id"],
                    "name": f["name"],
                    "path": os.path.join(prefix, f["name"]),
                    "modifiedTime": f.get("modifiedTime"),
                }
            )
    return entries


def baixar_arquivo(service, file_id: str, destino: str) -> None:
    """Baixa um único arquivo do Google Drive."""
    from googleapiclient.http import MediaIoBaseDownload
    from googleapiclient.errors import HttpError

    request = service.files().get_media(fileId=file_id)
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    with open(destino, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            try:
                status, done = downloader.next_chunk()
            except HttpError as e:
                log.error(f"Erro HTTP ao baixar arquivo {file_id}: {e}")
                break


def criar_servico_drive():
    """Wrapper para ``drive_utils.criar_servico_drive``."""
    return get_drive_service()


def baixar_xmls_empresa_zip(
    service,
    pasta_principal_id: str,
    nome_empresa: str,
    dest_dir: str,
) -> List[str]:
    """Baixa o arquivo ``*.zip`` da pasta da empresa e retorna os XMLs extraídos."""
    from utils.drive_utils import extrair_zip_seguro

    log.info(
        "Buscando pasta da empresa '%s' em '%s'", nome_empresa, pasta_principal_id
    )
    empresa_id = _find_subfolder(service, pasta_principal_id, nome_empresa)
    if not empresa_id:
        log.error("Pasta da empresa '%s' não encontrada", nome_empresa)
        raise FileNotFoundError(
            f"Pasta da empresa '{nome_empresa}' não encontrada no Drive"
        )
    log.info("Pasta da empresa encontrada: %s", empresa_id)

    arquivos = _list_files(service, empresa_id)
    arquivos_zip = [a for a in arquivos if a["name"].lower().endswith(".zip")]
    if not arquivos_zip:
        log.warning("Nenhum arquivo ZIP encontrado para '%s'", nome_empresa)
        return []

    zip_config = os.getenv("NOME_ARQUIVO_ZIP")
    if len(arquivos_zip) > 1:
        if zip_config:
            alvo = next((a for a in arquivos_zip if a["name"] == zip_config), None)
            if not alvo:
                log.warning(
                    "Nenhum ZIP corresponde ao nome configurado '%s' para a empresa '%s'",
                    zip_config,
                    nome_empresa,
                )
                raise FileNotFoundError(
                    f"Arquivo ZIP '{zip_config}' não encontrado para a empresa '{nome_empresa}'",
                )
        else:
            nomes = [a["name"] for a in arquivos_zip]
            log.warning(
                "Múltiplos arquivos ZIP encontrados para a empresa '%s': %s",
                nome_empresa,
                nomes,
            )
            raise RuntimeError(
                "Configure NOME_ARQUIVO_ZIP para selecionar o arquivo desejado.",
            )
    else:
        alvo = arquivos_zip[0]

    log.info("Arquivo ZIP escolhido: %s (id=%s)", alvo["name"], alvo["id"])
    
    # Criar um nome de arquivo único para o ZIP baixado
    zip_local_filename = f"{nome_empresa}_{alvo['name']}"
    zip_path = os.path.join(dest_dir, zip_local_filename)
    
    os.makedirs(dest_dir, exist_ok=True)
    baixar_arquivo(service, alvo["id"], zip_path)
    log.info("Download concluído: %s", zip_path)
    
    try:
        # Usar a função extrair_zip_seguro para extrair o ZIP
        xml_paths = extrair_zip_seguro(zip_path, dest_dir)
    except Exception as exc:
        log.exception(
            "Falha ao processar ZIP para a empresa '%s'", nome_empresa
        )
        raise

    if not xml_paths:
        log.error("Nenhum XML encontrado em %s", dest_dir)
        raise FileNotFoundError(f"Nenhum XML encontrado em {dest_dir}")
    log.info("XMLs extraídos: %s", [os.path.basename(x) for x in xml_paths])
    return xml_paths


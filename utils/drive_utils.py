"""Utilidades para integração com o Google Drive."""

from __future__ import annotations

import os
import logging
import zipfile
import unicodedata
from typing import List, Optional

import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


log = logging.getLogger(__name__)


def criar_servico_drive():
    """Cria um serviço de acesso ao Google Drive usando ``GCP_SERVICE_ACCOUNT_JSON``.

    A variável de ambiente deve conter o JSON completo da chave de serviço.
    """

    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    raw_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if not raw_json:
        raise EnvironmentError(
            "Variável GCP_SERVICE_ACCOUNT_JSON não definida"
        )
    try:
        info = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "Conteúdo inválido em GCP_SERVICE_ACCOUNT_JSON"
        ) from exc

    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return build("drive", "v3", credentials=creds)


def _buscar_subpasta_id(service, parent_id: str, nome: str) -> Optional[str]:
    """Retorna o ID de uma subpasta de ``parent_id`` com o ``nome`` informado."""

    def _normalizar(texto: str) -> str:
        texto_norm = unicodedata.normalize("NFD", texto)
        texto_sem_acento = "".join(
            c for c in texto_norm if unicodedata.category(c) != "Mn"
        )
        return texto_sem_acento.casefold()

    query = (
        f"'{parent_id}' in parents and "
        "mimeType='application/vnd.google-apps.folder' and "
        "trashed=false"
    )
    res = service.files().list(q=query, fields="files(id,name)").execute()
    for f in res.get("files", []):
        if _normalizar(f["name"]) == _normalizar(nome):
            return f["id"]
    return None


def listar_arquivos(service, pasta_id: str) -> List[dict]:
    """Lista arquivos (não pastas) dentro da pasta especificada."""

    query = (
        f"'{pasta_id}' in parents and mimeType!='application/vnd.google-apps.folder' "
        "and trashed=false"
    )
    arquivos = []
    page_token = None
    while True:
        res = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id,name,modifiedTime)",
                pageToken=page_token,
            )
            .execute()
        )
        arquivos.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return arquivos


def baixar_arquivo(service, file_id: str, destino: str) -> None:
    """Baixa um único arquivo do Google Drive."""

    request = service.files().get_media(fileId=file_id)
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    with open(destino, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            try:
                status, done = downloader.next_chunk()
            except HttpError:
                break


def normalizar_nome_arquivo(nome: str) -> str:
    """Normaliza nome de arquivo removendo acentos e caracteres especiais."""
    # Remove acentos
    nome_normalizado = unicodedata.normalize('NFD', nome)
    nome_sem_acento = ''.join(c for c in nome_normalizado if unicodedata.category(c) != 'Mn')
    
    # Remove caracteres especiais, mantém apenas letras, números, pontos, hífens e underscores
    nome_limpo = ''.join(c for c in nome_sem_acento if c.isalnum() or c in '._-')
    
    return nome_limpo


def extrair_zip_seguro(zip_path: str, dest_dir: str) -> List[str]:
    """Extrai ZIP de forma segura, prevenindo Zip Slip attacks."""
    xml_paths = []
    dest_dir = os.path.abspath(dest_dir)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                # Normalizar nome do arquivo
                nome_normalizado = normalizar_nome_arquivo(member)
                
                # Validação contra Zip Slip
                member_path = os.path.join(dest_dir, nome_normalizado)
                member_path = os.path.abspath(member_path)
                
                if not member_path.startswith(dest_dir):
                    log.warning(f"Tentativa de Zip Slip detectada, ignorando: {member}")
                    continue
                
                # Criar diretórios se necessário
                os.makedirs(os.path.dirname(member_path), exist_ok=True)
                
                # Extrair apenas arquivos XML
                if member.lower().endswith('.xml'):
                    try:
                        with zip_ref.open(member) as source, open(member_path, 'wb') as target:
                            target.write(source.read())
                        xml_paths.append(member_path)
                        log.info(f"XML extraído: {member} -> {nome_normalizado}")
                    except Exception as e:
                        log.error(f"Erro ao extrair {member}: {e}")
                        continue
                        
    except zipfile.BadZipFile as e:
        log.error(f"Arquivo ZIP inválido: {zip_path} - {e}")
        raise
    except Exception as e:
        log.error(f"Erro ao extrair ZIP {zip_path}: {e}")
        raise
    
    return xml_paths


def baixar_xmls_empresa_zip(
    service,
    pasta_principal_id: str,
    nome_empresa: str,
    destino: str,
) -> List[str]:
    """Baixa o arquivo ``*.zip`` da pasta da empresa e retorna os XMLs extraídos."""

    log.info(
        "Buscando pasta da empresa '%s' em '%s'", nome_empresa, pasta_principal_id
    )
    empresa_id = _buscar_subpasta_id(service, pasta_principal_id, nome_empresa)
    if not empresa_id:
        log.error("Pasta da empresa '%s' não encontrada", nome_empresa)
        raise FileNotFoundError(
            f"Pasta da empresa '{nome_empresa}' não encontrada no Drive"
        )
    log.info("Pasta da empresa encontrada: %s", empresa_id)

    arquivos = listar_arquivos(service, empresa_id)
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
    os.makedirs(destino, exist_ok=True)
    zip_path = os.path.join(destino, "empresa.zip")
    baixar_arquivo(service, alvo["id"], zip_path)
    log.info("Download concluído: %s", zip_path)
    
    try:
        xml_paths = extrair_zip_seguro(zip_path, destino)
    except zipfile.BadZipFile as exc:
        log.exception(
            "Falha ao processar ZIP para a empresa '%s'", nome_empresa
        )
        raise

    if not xml_paths:
        log.error("Nenhum XML encontrado em %s", destino)
        raise FileNotFoundError(f"Nenhum XML encontrado em {destino}")
    log.info("XMLs extraídos: %s", [os.path.basename(x) for x in xml_paths])
    return xml_paths


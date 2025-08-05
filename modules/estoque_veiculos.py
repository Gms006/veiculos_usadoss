import os
import re
import json
import logging
import zipfile
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Tuple

import pandas as pd
import xml.etree.ElementTree as ET

log = logging.getLogger(__name__)

# Caminhos de configuração
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config')

# Carregamento de configurações
try:
    with open(os.path.join(CONFIG_PATH, 'extracao_config.json'), encoding='utf-8') as f:
        CONFIG_EXTRACAO = json.load(f)

    with open(os.path.join(CONFIG_PATH, 'layout_colunas.json'), encoding='utf-8') as f:
        LAYOUT_COLUNAS = json.load(f)
except Exception as e:
    log.error(f"Erro ao carregar arquivos de configuração: {e}")
    # Definir configurações padrão caso ocorra erro na leitura
    CONFIG_EXTRACAO = {
        "validadores": {
            "chassi": r'^[A-HJ-NPR-Z0-9]{17}$',
            "placa_mercosul": r'^[A-Z]{3}[0-9][A-Z][0-9]{2}$',
            "placa_antiga": r'^[A-Z]{3}[0-9]{4}$',
            "renavam": r'^\d{9,11}$'
        },
        "xpath_campos": {
            "CFOP": ".//nfe:det/nfe:prod/nfe:CFOP",
            "Data Emissão": ".//nfe:ide/nfe:dhEmi",
            "Emitente CNPJ": ".//nfe:emit/nfe:CNPJ",
            "Emitente CPF": ".//nfe:emit/nfe:CPF",
            "Destinatário CNPJ": ".//nfe:dest/nfe:CNPJ",
            "Destinatário CPF": ".//nfe:dest/nfe:CPF",
            "Valor Total": ".//nfe:total/nfe:ICMSTot/nfe:vNF",
            "Produto": ".//nfe:det/nfe:prod/nfe:xProd",
            "Natureza Operação": ".//nfe:ide/nfe:natOp"
        },
        "regex_extracao": {
            "Chassi": r'(?:CHASSI|CHAS|CH)[\s:;.-]*([A-HJ-NPR-Z0-9]{17})',
            "Placa": r'(?:PLACA|PL)[\s:;.-]*([A-Z]{3}[0-9][A-Z0-9][0-9]{2})|(?:PLACA|PL)[\s:;.-]*([A-Z]{3}-?[0-9]{4})',
            "Renavam": r'(?:RENAVAM|REN|RENAV)[\s:;.-]*([0-9]{9,11})',
            "KM": r'(?:KM|QUILOMETRAGEM|HODOMETRO|HODÔMETRO)[\s:;.-]*([0-9]{1,7})',
            "Ano Modelo": r'(?:ANO[\s/]*MODELO|ANO[\s/]?FAB[\s/]?MOD)[\s:;.-]*([0-9]{4})[\s/.-]+([0-9]{4})|ANO[\s:;.-]*([0-9]{4})[\s/.-]+([0-9]{4})',
            "Cor": r'(?:COR|COLOR)[\s:;.-]*([A-Za-zÀ-ú\s]+?)(?:[\s,.;]|$)',
            "Motor": r'(?:MOTOR|MOT|N[º°\s]?\s*MOTOR)[\s:;.-]*([A-Z0-9]+)',
            "Combustível": r'(?:COMBUSTÍVEL|COMBUSTIVEL|COMB)[\s:;.-]*([A-Za-zÀ-ú\s/]+?)(?:[\s,.;]|$)',
            "Modelo": r'(?:MODELO|MOD)[\s:;.-]*([A-Za-zÀ-ú0-9\s\.-]+?)(?:[\s,.;]|$)',
            "Potência": r'(?:POTÊNCIA|POTENCIA|POT)[\s:;.-]*([0-9]+(?:[,.][0-9]+)?)'
        }
    }
    LAYOUT_COLUNAS = {
        "CFOP": {"tipo": "str", "ordem": 1},
        "Data Emissão": {"tipo": "date", "ordem": 2},
        "Emitente CNPJ/CPF": {"tipo": "str", "ordem": 3},
        "Destinatário CNPJ/CPF": {"tipo": "str", "ordem": 4},
        "Chassi": {"tipo": "str", "ordem": 5},
        "Placa": {"tipo": "str", "ordem": 6},
        "Produto": {"tipo": "str", "ordem": 7},
        "Valor Total": {"tipo": "float", "ordem": 8},
        "Renavam": {"tipo": "str", "ordem": 9},
        "KM": {"tipo": "int", "ordem": 10},
        "Ano Modelo": {"tipo": "int", "ordem": 11},
        "Ano Fabricação": {"tipo": "int", "ordem": 12},
        "Cor": {"tipo": "str", "ordem": 13},
        "Motor": {"tipo": "str", "ordem": 14},
        "Combustível": {"tipo": "str", "ordem": 15},
        "Potência": {"tipo": "float", "ordem": 16},
        "Modelo": {"tipo": "str", "ordem": 17},
        "Natureza Operação": {"tipo": "str", "ordem": 99},
        "CHAVE XML": {"tipo": "str", "ordem": 100}
    }

# Pré-compilar as expressões regulares para melhor performance
REGEX_COMPILADOS = {}
try:
    for campo, padrao in CONFIG_EXTRACAO["regex_extracao"].items():
        REGEX_COMPILADOS[campo] = re.compile(padrao, re.IGNORECASE)
    log.info("Expressões regulares compiladas com sucesso")
except Exception as e:
    log.error(f"Erro ao compilar expressões regulares: {e}")
    REGEX_COMPILADOS = {}

# Funções de validação
def validar_chassi(chassi: Optional[str]) -> bool:
    """Valida o formato do chassi."""
    if not chassi:
        return False
    chassi = re.sub(r'\W', '', str(chassi)).upper()
    pattern = re.compile(CONFIG_EXTRACAO["validadores"]["chassi"])
    return bool(pattern.fullmatch(chassi))

def validar_placa(placa: Optional[str]) -> bool:
    """Valida o formato da placa (mercosul ou antiga)."""
    if not placa:
        return False
    placa = str(placa).strip().upper()
    placa_sem_hifen = placa.replace('-', '')
    
    # Validar formato Mercosul
    pattern_mercosul = re.compile(CONFIG_EXTRACAO["validadores"]["placa_mercosul"])
    if pattern_mercosul.fullmatch(placa_sem_hifen):
        return True
    
    # Validar formato antigo
    pattern_antigo = re.compile(CONFIG_EXTRACAO["validadores"]["placa_antiga"].replace('-', ''))
    if pattern_antigo.fullmatch(placa_sem_hifen):
        return True
    
    return False

def validar_renavam(renavam: Optional[str]) -> bool:
    """Valida o formato do renavam."""
    if not renavam:
        return False
    renavam = str(renavam).strip()
    # Remove caracteres não numéricos
    renavam = re.sub(r'\D', '', renavam)
    pattern = re.compile(CONFIG_EXTRACAO["validadores"].get("renavam", r'^\d{9,11}$'))
    return bool(pattern.fullmatch(renavam))

def classificar_tipo_nota(
    emitente_cnpj: Optional[str],
    destinatario_cnpj: Optional[str],
    cnpj_empresa: Union[str, List[str], None],
    cfop: Optional[str],
    *,
    retornar_alerta: bool = False,
) -> Union[str, Tuple[str, str]]:
    """Classifica a nota como ``Entrada``, ``Saída`` ou ``Indefinido`` e gera alertas.

    Regras principais:
    1. Se o destinatário for a empresa, sempre ``Entrada``.
    2. Se o emitente for a empresa e o CFOP começar com ``5``, ``6`` ou ``7``, é
       ``Saída``.
    3. Se o emitente for a empresa e o CFOP for de entrada (``1``, ``2`` ou
       ``3``), é ``Entrada`` com alerta de possível erro.
    4. Nos demais casos o resultado é ``Indefinido``. Caso o CFOP indique
       entrada mas a empresa não esteja envolvida, registra alerta.
    """

    emitente = normalizar_cnpj(emitente_cnpj)
    destinatario = normalizar_cnpj(destinatario_cnpj)

    if isinstance(cnpj_empresa, (list, tuple, set)):
        cnpjs_empresa = {normalizar_cnpj(c) for c in cnpj_empresa if normalizar_cnpj(c)}
    elif cnpj_empresa:
        cnpjs_empresa = {normalizar_cnpj(cnpj_empresa)}
    else:
        cnpjs_empresa = set()

    emit_e_empresa = emitente in cnpjs_empresa if emitente else False
    dest_e_empresa = destinatario in cnpjs_empresa if destinatario else False

    alerta = ""

    cfop_str = ""
    if cfop is not None:
        try:
            cfop_str = re.sub(r"\D", "", str(cfop))
            if len(cfop_str) > 4:
                cfop_str = cfop_str[:4]
            cfop_str = cfop_str.strip()
        except Exception:
            cfop_str = ""

    cfop_ini = cfop_str[0] if cfop_str else ""

    if dest_e_empresa:
        tipo = "Entrada"
        if emit_e_empresa and cfop_ini in {"1", "2", "3"}:
            alerta = (
                "Entrada emitida pela própria empresa, possível erro de emissão."
            )
        if retornar_alerta:
            return tipo, alerta
        return tipo

    if emit_e_empresa:
        if cfop_ini in {"5", "6", "7"}:
            tipo = "Saída"
        elif cfop_ini in {"1", "2", "3"}:
            tipo = "Entrada"
            alerta = (
                "Entrada emitida pela própria empresa, possível erro de emissão."
            )
        else:
            tipo = "Indefinido"
        if retornar_alerta:
            return tipo, alerta
        return tipo

    tipo = "Indefinido"
    if cfop_ini in {"1", "2", "3"}:
        alerta = "Nota não envolve a empresa, mas CFOP é de entrada. Verificar!"

    if retornar_alerta:
        return tipo, alerta
    return tipo

def classificar_produto(row: Dict[str, Any]) -> str:
    """Classifica o item como veículo apenas se houver chassi."""

    chassi = row.get("Chassi")
    if chassi is not None and validar_chassi(chassi):
        return "Veículo"

    return "Consumo"

def limpar_texto(texto: Optional[str]) -> str:
    """Remove caracteres especiais e espaços extras."""
    if not texto:
        return ""
    texto = str(texto).strip()
    texto = re.sub(r'\s+', ' ', texto)  # Remove espaços extras
    return texto

def formatar_data(data_str: Optional[str]) -> Optional[datetime]:
    """Converte strings de data em objetos ``datetime``.

    Manter as datas como ``datetime`` evita conversões repetidas durante as
    agregações mensais.
    """
    if not data_str:
        return None
    try:
        for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
            try:
                data_str_limpa = re.sub(r"[-+]\d{2}:\d{2}$", "", data_str)
                return datetime.strptime(data_str_limpa, fmt)
            except ValueError:
                continue
    except Exception as e:
        log.warning(f"Erro ao converter data '{data_str}': {e}")
    return None

def extrair_placa(texto_completo: str) -> Optional[str]:
    """Extrai a placa de veículo usando regex."""
    if not texto_completo:
        return None

    # Usar regex pré-compilado se disponível
    if 'Placa' in REGEX_COMPILADOS:
        match = REGEX_COMPILADOS['Placa'].search(texto_completo)
        if match:
            # Verificar qual dos grupos capturou algo (formato mercosul ou antigo)
            for grupo in match.groups():
                if grupo:
                    placa = grupo.strip().upper()
                    if validar_placa(placa):
                        return placa
    else:
        # Fallback para regex não compilado
        padrao = CONFIG_EXTRACAO["regex_extracao"]["Placa"]
        match = re.search(padrao, texto_completo, re.IGNORECASE)
        if match:
            # Pegar o primeiro grupo não vazio
            for grupo in match.groups():
                if grupo:
                    placa = grupo.strip().upper()
                    if validar_placa(placa):
                        return placa

    return None

def extrair_info_com_regex(texto_completo: str, campo: str) -> Optional[str]:
    """Extrai informações usando regex em um texto."""
    if not texto_completo or not campo:
        return None
    
    # Caso especial para Placa que tem um padrão mais complexo
    if campo == "Placa":
        return extrair_placa(texto_completo)
    
    # Usar regex pré-compilado se disponível
    if campo in REGEX_COMPILADOS:
        match = REGEX_COMPILADOS[campo].search(texto_completo)
    else:
        # Fallback para regex não compilado
        padrao = CONFIG_EXTRACAO["regex_extracao"].get(campo)
        if not padrao:
            return None
        match = re.search(padrao, texto_completo, re.IGNORECASE)
    
    if not match:
        log.debug(f"Nenhum match encontrado para o campo {campo} com o texto: {texto_completo}")
        return None
    
    log.debug(f"Match encontrado para o campo {campo}. Grupos: {match.groups()}")

    # Para o caso de Ano Modelo que tem dois formatos possíveis
    if campo == "Ano Modelo" and match.groups():
        # Verifica qual formato foi usado
        if match.group(1) and match.group(2):  # Formato principal
            return match.group(2)  # Retorna o ano modelo
        elif match.group(3) and match.group(4):  # Formato alternativo
            return match.group(4)  # Retorna o ano modelo
    
    # Para Chassi, que tem apenas um grupo de captura
    if campo == "Chassi":
        # A regex do chassi tem apenas um grupo de captura, que é o próprio chassi
        log.debug(f"Extraindo chassi: {match.group(1)}")
        return str(match.group(1)).strip() if match.group(1) else None

    # Para campos normais, retorna o primeiro grupo de captura não vazio
    for group_val in match.groups():
        if group_val is not None:
            return str(group_val).strip()
    
    return None

def normalizar_cnpj(cnpj: Optional[str]) -> Optional[str]:
    """Remove formatação do CNPJ e retorna apenas os números."""
    if not cnpj:
        return None
    return re.sub(r'\D', '', str(cnpj))

def normalizar_nome_arquivo(nome: str) -> str:
    """Normaliza nome de arquivo removendo acentos e caracteres especiais."""
    # Remove acentos
    nome_normalizado = unicodedata.normalize('NFD', nome)
    nome_sem_acento = ''.join(c for c in nome_normalizado if unicodedata.category(c) != 'Mn')
    
    # Remove caracteres especiais, mantém apenas letras, números, pontos, hífens e underscores
    nome_limpo = re.sub(r'[^\w\.-]', '_', nome_sem_acento)
    
    return nome_limpo

def safe_parse_xml(xml_path: str, base_dir: Optional[str] = None) -> Tuple[Optional[ET.ElementTree], Optional[str]]:
    """Parse seguro de XML com tratamento de encoding e validação de path."""
    if not os.path.exists(xml_path):
        log.warning(f"XML não encontrado, pulando: {xml_path}")
        return None, f"Não encontrado: {xml_path}"
    
    # Validação de segurança contra Zip Slip
    xml_path_abs = os.path.abspath(xml_path)
    
    # Se base_dir não for fornecido, usa o diretório atual do script
    if base_dir is None:
        base_dir = os.path.abspath(os.getcwd())
    else:
        base_dir = os.path.abspath(base_dir)

    if not xml_path_abs.startswith(base_dir):
        log.error(f"Tentativa de acesso a arquivo fora do diretório permitido: {xml_path}")
        return None, f"Acesso negado: {xml_path}"
    
    try:
        with open(xml_path, "rb") as f:
            data = f.read()
        
        # Tenta diferentes encodings
        for enc in ("utf-8", "latin-1", "iso-8859-1"):
            try:
                text = data.decode(enc)
                tree = ET.ElementTree(ET.fromstring(text))
                return tree, None
            except UnicodeDecodeError:
                continue
            except ET.ParseError as e:
                log.error(f"Erro de parse em {xml_path}: {e}")
                return None, f"ParseError: {xml_path} -> {e}"
        
        log.error(f"Falha de encoding ao ler {xml_path}")
        return None, f"EncodingError: {xml_path}"
    except OSError as e:
        log.error(f"Erro de leitura em {xml_path}: {e}")
        return None, f"IOError: {xml_path} -> {e}"

def extrair_dados_xml(xml_path: str, erros_xml: List[str] = None, base_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """Extrai dados de um arquivo XML de NFe."""
    if erros_xml is None:
        erros_xml = []
    
    tree, err = safe_parse_xml(xml_path, base_dir=base_dir)
    if err:
        erros_xml.append(err)
        return []
    
    try:
        log.info(f"Processando XML: {xml_path}")
        root = tree.getroot()
        
        # Detectar namespace automaticamente
        ns_match = re.match(r'\{(.+?)\}', root.tag)
        ns_uri = ns_match.group(1) if ns_match else ''
        ns = {'nfe': ns_uri} if ns_uri else {}
        
        # Log para debug do namespace
        log.debug(f"Namespace detectado: {ns}")

        # Obter número da NF para referência em logs
        try:
            if ns:
                xpath_num_nf = ".//nfe:ide/nfe:nNF"
                num_nf = root.findtext(xpath_num_nf, namespaces=ns) or "Desconhecido"
            else:
                xpath_num_nf = ".//ide/nNF"
                num_nf = root.findtext(xpath_num_nf) or "Desconhecido"
            log.info(f"Processando NF número: {num_nf}")
        except Exception as e:
            log.warning(f"Erro ao obter número da NF: {e}")
            num_nf = "Desconhecido"

        # Chave de acesso do XML
        chave_xml = ""
        try:
            if ns:
                inf_nfe = root.find('.//nfe:infNFe', namespaces=ns)
            else:
                inf_nfe = root.find('.//infNFe')
            if inf_nfe is not None:
                chave_xml = inf_nfe.attrib.get('Id', '')
        except Exception:
            chave_xml = ""

        # Extrair dados dos campos XPath do cabeçalho da nota
        
        # Garantir campos do cabeçalho sempre preenchidos
        if ns:
            data_emissao_text = (
                root.findtext('.//nfe:ide/nfe:dhEmi', namespaces=ns)
                or root.findtext('.//nfe:ide/nfe:dEmi', namespaces=ns)
            )
            emit_cnpj = root.findtext('.//nfe:emit/nfe:CNPJ', namespaces=ns) or ""
            emit_cpf = root.findtext('.//nfe:emit/nfe:CPF', namespaces=ns) or ""
            dest_cnpj = root.findtext('.//nfe:dest/nfe:CNPJ', namespaces=ns) or ""
            dest_cpf = root.findtext('.//nfe:dest/nfe:CPF', namespaces=ns) or ""
            cfop = (
                root.findtext('.//nfe:det/nfe:prod/nfe:CFOP', namespaces=ns)
                or root.findtext('.//CFOP', namespaces=ns)
            )
            valor_total = root.findtext('.//nfe:total/nfe:ICMSTot/nfe:vNF', namespaces=ns)
            natureza_operacao = root.findtext('.//nfe:ide/nfe:natOp', namespaces=ns)
        else:
            data_emissao_text = (
                root.findtext('.//ide/dhEmi')
                or root.findtext('.//ide/dEmi')
            )
            emit_cnpj = root.findtext('.//emit/CNPJ') or ""
            emit_cpf = root.findtext('.//emit/CPF') or ""
            dest_cnpj = root.findtext('.//dest/CNPJ') or ""
            dest_cpf = root.findtext('.//dest/CPF') or ""
            cfop = (
                root.findtext('.//det/prod/CFOP')
                or root.findtext('.//CFOP')
            )
            valor_total = root.findtext('.//total/ICMSTot/vNF')
            natureza_operacao = root.findtext('.//ide/natOp')
        
        data_emissao = formatar_data(data_emissao_text)
        emit_id = emit_cnpj.strip() or emit_cpf.strip() or "Não informado"
        dest_id = dest_cnpj.strip() or dest_cpf.strip() or "Não informado"

        cabecalho = {
            'Número NF': num_nf,
            'CHAVE XML': chave_xml,
            'Emitente CNPJ/CPF': normalizar_cnpj(emit_id),
            'Destinatário CNPJ/CPF': normalizar_cnpj(dest_id),
            'CFOP': cfop,
            'Data Emissão': data_emissao,
            'Mês Emissão': data_emissao.strftime('%m/%Y') if data_emissao else None,
            'Valor Total': valor_total,
            'Natureza Operação': natureza_operacao,
        }
        log.debug(f"Cabeçalho extraído: {cabecalho}")

        registros = []
        # Campos padrão baseados nas chaves do LAYOUT_COLUNAS + campos adicionais
        campos_padrao = list(LAYOUT_COLUNAS.keys()) + ['Produto', 'XML Path', 'Item', 'Valor Item']

        # Procura por itens (produtos) na NFe
        if ns:
            itens = root.findall('.//nfe:det', ns)
        else:
            itens = root.findall('.//det')
        log.info(f"Encontrados {len(itens)} itens na NF")

        # Extrair informações adicionais de ICMS
        def extrair_icms_info(item_elem):
            """Extrai informações de ICMS do item."""
            icms_info = {}
            try:
                # Buscar informações de ICMS
                if ns:
                    icms_elem = item_elem.find('.//nfe:ICMS', ns)
                else:
                    icms_elem = item_elem.find('.//ICMS')
                if icms_elem is not None:
                    # Pode ter diferentes tipos de ICMS (ICMS00, ICMS10, etc.)
                    for icms_tipo in icms_elem:
                        if icms_tipo.tag.endswith('}ICMS00') or icms_tipo.tag.endswith('}ICMS10') or 'ICMS' in icms_tipo.tag:
                            if ns:
                                icms_info['ICMS Alíquota'] = icms_tipo.findtext('.//nfe:pICMS', namespaces=ns)
                                icms_info['ICMS Valor'] = icms_tipo.findtext('.//nfe:vICMS', namespaces=ns)
                                icms_info['ICMS Base'] = icms_tipo.findtext('.//nfe:vBC', namespaces=ns)
                                icms_info['CST ICMS'] = icms_tipo.findtext('.//nfe:CST', namespaces=ns) or icms_tipo.findtext('.//nfe:CSOSN', namespaces=ns)
                                icms_info['Redução BC'] = icms_tipo.findtext('.//nfe:pRedBC', namespaces=ns)
                                icms_info['Modalidade BC'] = icms_tipo.findtext('.//nfe:modBC', namespaces=ns)
                            else:
                                icms_info['ICMS Alíquota'] = icms_tipo.findtext('.//pICMS')
                                icms_info['ICMS Valor'] = icms_tipo.findtext('.//vICMS')
                                icms_info['ICMS Base'] = icms_tipo.findtext('.//vBC')
                                icms_info['CST ICMS'] = icms_tipo.findtext('.//CST') or icms_tipo.findtext('.//CSOSN')
                                icms_info['Redução BC'] = icms_tipo.findtext('.//pRedBC')
                                icms_info['Modalidade BC'] = icms_tipo.findtext('.//modBC')
                            break
            except Exception as e:
                log.warning(f"Erro ao extrair informações de ICMS: {e}")
            
            return icms_info

        if itens:
            for i, item in enumerate(itens, 1):
                try:
                    # Dados básicos do item
                    if ns:
                        produto_elem = item.find('.//nfe:prod', ns)
                    else:
                        produto_elem = item.find('.//prod')
                    if produto_elem is None:
                        continue
                    
                    if ns:
                        produto_descricao = produto_elem.findtext('.//nfe:xProd', namespaces=ns) or ""
                        valor_item = produto_elem.findtext('.//nfe:vProd', namespaces=ns) or ""
                        cfop_item = produto_elem.findtext('.//nfe:CFOP', namespaces=ns) or cabecalho.get('CFOP', "")
                    else:
                        produto_descricao = produto_elem.findtext('.//xProd') or ""
                        valor_item = produto_elem.findtext('.//vProd') or ""
                        cfop_item = produto_elem.findtext('.//CFOP') or cabecalho.get('CFOP', "")
                    
                    # Extrair informações de ICMS
                    icms_info = extrair_icms_info(item)
                    
                    # Criar registro base
                    registro = {**cabecalho}
                    registro.update({
                        'Item': i,
                        'Produto': produto_descricao,
                        'Valor Item': valor_item,
                        'CFOP': cfop_item,
                        'XML Path': xml_path,
                        **icms_info
                    })
                    
                    # Extrair informações específicas de veículos usando regex
                    texto_completo = produto_descricao
                    
                    # Campos de veículos para extração
                    campos_veiculo = ['Chassi', 'Placa', 'Renavam', 'KM', 'Ano Modelo', 'Cor', 'Motor', 'Combustível', 'Modelo', 'Potência']
                    
                    for campo in campos_veiculo:
                        valor_extraido = extrair_info_com_regex(texto_completo, campo)
                        if valor_extraido:
                            registro[campo] = valor_extraido
                        else:
                            # Garante que o campo existe no registro, mesmo que vazio
                            registro[campo] = None
                    
                    # Tratamento especial para Ano Fabricação (derivado de Ano Modelo)
                    if registro.get('Ano Modelo'):
                        try:
                            ano_modelo = int(registro['Ano Modelo'])
                            # Geralmente o ano de fabricação é o ano anterior ao modelo
                            registro['Ano Fabricação'] = ano_modelo - 1
                        except (ValueError, TypeError):
                            registro['Ano Fabricação'] = None
                    
                    # Garantir que todos os campos do layout estejam presentes
                    for campo in campos_padrao:
                        if campo not in registro:
                            registro[campo] = None
                    
                    registros.append(registro)
                    log.debug(f"Item {i} processado: {produto_descricao[:50]}...")
                    
                except Exception as e:
                    log.error(f"Erro ao processar item {i} do XML {xml_path}: {e}")
                    continue
        else:
            # Se não há itens, criar um registro apenas com dados do cabeçalho
            registro = {**cabecalho}
            registro.update({
                'Item': 1,
                'XML Path': xml_path,
            })
            
            # Garantir que todos os campos do layout estejam presentes
            for campo in campos_padrao:
                if campo not in registro:
                    registro[campo] = None
            
            registros.append(registro)
            log.info("Nenhum item encontrado, criando registro apenas com cabeçalho")

        log.info(f"XML processado com sucesso: {len(registros)} registros extraídos")
        return registros

    except Exception as e:
        erro_msg = f"Erro geral ao processar XML {xml_path}: {e}"
        log.error(erro_msg)
        erros_xml.append(erro_msg)
        return []

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

def processar_xmls(xml_paths: List[str], cnpj_empresa: str, base_dir: Optional[str] = None) -> pd.DataFrame:
    """Processa uma lista de arquivos XML e retorna um DataFrame consolidado."""
    if not xml_paths:
        log.warning("Nenhum arquivo XML fornecido para processamento")
        return pd.DataFrame()
    
    log.info(f"Iniciando processamento de {len(xml_paths)} arquivos XML")
    
    todos_registros = []
    erros_xml = []
    
    for xml_path in xml_paths:
        try:
            registros = extrair_dados_xml(xml_path, erros_xml, base_dir=base_dir)
            todos_registros.extend(registros)
        except Exception as e:
            erro_msg = f"Erro crítico ao processar {xml_path}: {e}"
            log.error(erro_msg)
            erros_xml.append(erro_msg)
            continue
    
    if not todos_registros:
        log.warning("Nenhum registro extraído dos XMLs")
        return pd.DataFrame()
    
    # Criar DataFrame
    df = pd.DataFrame(todos_registros)
    log.info(f"DataFrame criado com {len(df)} registros")
    
    # Classificar tipo de nota
    df['Tipo Nota'] = df.apply(
        lambda row: classificar_tipo_nota(
            row.get('Emitente CNPJ/CPF'),
            row.get('Destinatário CNPJ/CPF'),
            cnpj_empresa,
            row.get('CFOP')
        ),
        axis=1
    )
    
    # Classificar produto
    df['Classificação'] = df.apply(classificar_produto, axis=1)
    
    # Log de estatísticas
    log.info(f"Tipos de nota: {df['Tipo Nota'].value_counts().to_dict()}")
    log.info(f"Classificações: {df['Classificação'].value_counts().to_dict()}")
    
    if erros_xml:
        log.warning(f"Total de erros durante o processamento: {len(erros_xml)}")
        for erro in erros_xml[:5]:  # Mostrar apenas os primeiros 5 erros
            log.warning(f"Erro: {erro}")
    
    return df


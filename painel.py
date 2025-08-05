import os
import argparse
import logging
import pandas as pd
from datetime import datetime
import shutil
import tempfile

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Importações relativas
try:
    from modules.estoque_veiculos import processar_xmls, extrair_zip_seguro
    from modules.transformadores_veiculos import ( 
        consolidar_dados_veiculos,
        calcular_kpis_financeiros,
        gerar_relatorio_excel,
        validar_dados_finais
    )
except ImportError as e:
    log.error(f"Erro ao importar módulos: {e}. Verifique se as dependências estão corretas e o PYTHONPATH configurado.")
    exit(1)

def main():
    parser = argparse.ArgumentParser(description="Processa XMLs de NF-e para gerar relatórios de estoque de veículos.")
    parser.add_argument("--cnpj", required=True, help="CNPJ da empresa para classificação de notas.")
    parser.add_argument("--xml-dir", help="Diretório contendo os arquivos XML.")
    parser.add_argument("--zip-file", help="Caminho para o arquivo ZIP contendo os XMLs.")
    parser.add_argument("--output", default="relatorio_nfe.xlsx", help="Caminho para o arquivo de saída Excel.")

    args = parser.parse_args()

    if not args.xml_dir and not args.zip_file:
        log.error("É necessário fornecer --xml-dir ou --zip-file.")
        parser.print_help()
        return

    xml_paths = []
    temp_dir = None

    if args.zip_file:
        if not os.path.exists(args.zip_file):
            log.error(f"Arquivo ZIP não encontrado: {args.zip_file}")
            return
        
        try:
            temp_dir = tempfile.mkdtemp(prefix="nfe_extract_")
            log.info(f"Extraindo ZIP para diretório temporário: {temp_dir}")
            xml_paths = extrair_zip_seguro(args.zip_file, temp_dir)
        except Exception as e:
            log.error(f"Erro ao extrair arquivo ZIP: {e}")
            if temp_dir: shutil.rmtree(temp_dir)
            return
    elif args.xml_dir:
        if not os.path.isdir(args.xml_dir):
            log.error(f"Diretório XML não encontrado: {args.xml_dir}")
            return
        for root, _, files in os.walk(args.xml_dir):
            for file in files:
                if file.lower().endswith(".xml"):
                    xml_paths.append(os.path.join(root, file))
    
    if not xml_paths:
        log.warning("Nenhum arquivo XML encontrado para processamento.")
        if temp_dir: shutil.rmtree(temp_dir)
        return

    log.info(f"Processando {len(xml_paths)} arquivos XML")

    try:
        # Passa o diretório base para a função processar_xmls
        df_processado = processar_xmls(xml_paths, args.cnpj, base_dir=temp_dir if temp_dir else args.xml_dir)

        if df_processado.empty:
            log.warning("DataFrame vazio após processamento")
            log.warning("Nenhum dado processado.")
            log.error("Falha no processamento")
            return

        df_consolidado = consolidar_dados_veiculos(df_processado)
        kpis = calcular_kpis_financeiros(df_consolidado)
        
        # Validação final dos dados
        if not validar_dados_finais(df_consolidado):
            log.error("Validação final dos dados falhou. O relatório pode estar incompleto ou incorreto.")
            # Continua para gerar o relatório mesmo com falha na validação, mas com alerta

        gerar_relatorio_excel(df_consolidado, kpis, args.output)
        log.info(f"Relatório salvo em: {args.output}")
        log.info("=== KPIs ===")
        for k, v in kpis.items():
            log.info(f"{k}: {v}")
        log.info("Pipeline executado com sucesso")

    except Exception as e:
        log.error(f"Erro durante o pipeline de processamento: {e}", exc_info=True)
        log.error("Falha no processamento")
    finally:
        if temp_dir:
            log.info(f"Removendo diretório temporário: {temp_dir}")
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()



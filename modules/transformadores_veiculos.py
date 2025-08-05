import pandas as pd
import logging
import re

# Configuração de logging
log = logging.getLogger(__name__)

# Gerar Estoque Fiscal
def _limpar_chave(valor: str) -> str:
    """Remove caracteres não alfanuméricos e aplica caixa alta."""
    if valor is None:
        return ""
    return re.sub(r"\\W", "", str(valor)).upper()

def consolidar_dados_veiculos(df_processado: pd.DataFrame) -> pd.DataFrame:
    """Consolida os dados processados, garantindo tipos corretos e colunas essenciais.

    Args:
        df_processado (pd.DataFrame): DataFrame com os dados extraídos dos XMLs.

    Returns:
        pd.DataFrame: DataFrame consolidado e com tipos de dados ajustados.
    """
    log.info("Iniciando consolidação dos dados de veículos.")

    # Lista de colunas esperadas e seus tipos
    colunas_esperadas = {
        "CFOP": str,
        "Data Emissão": "datetime64[ns]",
        "Emitente CNPJ/CPF": str,
        "Destinatário CNPJ/CPF": str,
        "Chassi": str,
        "Placa": str,
        "Produto": str,
        "Valor Total": float,
        "Renavam": str,
        "KM": float, # Pode ser int, mas float para lidar com NaNs
        "Ano Modelo": float,
        "Ano Fabricação": float,
        "Cor": str,
        "Motor": str,
        "Combustível": str,
        "Potência": float,
        "Modelo": str,
        "Natureza Operação": str,
        "CHAVE XML": str,
        "Item": int,
        "Valor Item": float,
        "Tipo Nota": str,
        "Classificação": str,
        "ICMS Alíquota": float,
        "ICMS Valor": float,
        "ICMS Base": float,
        "CST ICMS": str,
        "Redução BC": float,
        "Modalidade BC": str,
        "Situação": str # Adicionando Situação aqui para garantir que sempre exista
    }

    # Garantir que todas as colunas esperadas existam, preenchendo com NaN se não
    for col, dtype in colunas_esperadas.items():
        if col not in df_processado.columns:
            df_processado[col] = pd.NA

    # Ajustar tipos de dados
    for col, dtype in colunas_esperadas.items():
        if col in df_processado.columns:
            try:
                if dtype == "datetime64[ns]":
                    df_processado[col] = pd.to_datetime(df_processado[col], errors=\'coerce\')
                else:
                    df_processado[col] = df_processado[col].astype(dtype, errors=\'ignore\')
            except Exception as e:
                log.warning(f"Não foi possível converter a coluna {col} para {dtype}: {e}")

    # Filtrar apenas veículos para as operações subsequentes, se a coluna \'Classificação\' existir
    if \'Classificação\' in df_processado.columns:
        df_veiculos = df_processado[df_processado["Classificação"] == "Veículo"].copy()
    else:
        log.warning("Coluna \'Classificação\' não encontrada. Processando todos os itens.")
        df_veiculos = df_processado.copy()

    # Adicionar colunas de Chave para merge
    df_veiculos["Chave"] = df_veiculos["Chassi"].apply(_limpar_chave)
    df_veiculos.loc[df_veiculos["Chave"] == "", "Chave"] = (
        df_veiculos["Placa"].apply(_limpar_chave)
    )

    log.info(f"Dados consolidados. Total de {len(df_veiculos)} veículos para análise.")
    return df_veiculos

def gerar_estoque_fiscal(df_entrada: pd.DataFrame, df_saida: pd.DataFrame) -> pd.DataFrame:
    """Gera o estoque fiscal de veículos a partir das notas de entrada e saída.

    Args:
        df_entrada (pd.DataFrame): DataFrame com as notas de entrada de veículos.
        df_saida (pd.DataFrame): DataFrame com as notas de saída de veículos.

    Returns:
        pd.DataFrame: DataFrame com o estoque fiscal, incluindo a situação (Vendido/Em Estoque/Erro).
    """
    log.info("Iniciando geração do estoque fiscal.")

    # Remover duplicidades explícitas de saída para evitar múltiplas associações
    # Usar \'CHAVE XML\' para identificar notas únicas, se disponível
    if \'CHAVE XML\' in df_saida.columns:
        antes = len(df_saida)
        df_saida = df_saida.drop_duplicates(subset=["CHAVE XML"])
        removidas = antes - len(df_saida)
        if removidas:
            log.info(f"Removidas {removidas} linhas duplicadas na saída com base na CHAVE XML.")
    else:
        log.warning("Coluna \'CHAVE XML\' não encontrada em df_saida. Duplicidades podem não ser removidas adequadamente.")

    # Usar junção externa para identificar saídas sem entradas
    df_estoque = pd.merge(
        df_entrada,
        df_saida,
        on="Chave",
        how="outer",
        suffixes=("_entrada", "_saida"),
        indicator=True,
    )

    # Classificação do status do veículo
    def classificar_status(merge_flag):
        if merge_flag == "both":
            return "Vendido"
        elif merge_flag == "left_only":
            return "Em Estoque"
        elif merge_flag == "right_only":
            return "Saída sem Entrada" # Nova categoria para saídas sem correspondência
        return "Erro"

    df_estoque["Situação"] = df_estoque["_merge"].apply(classificar_status)

    # Renomear coluna de data de saída, se existir
    if "Data Emissão_saida" in df_estoque.columns:
        df_estoque.rename(columns={"Data Emissão_saida": "Data Saída"}, inplace=True)
    else:
        df_estoque["Data Saída"] = pd.NaT # Not a Time

    def _obter_valor(df, prefixo):
        col_total = f"Valor Total_{prefixo}"
        col_item = f"Valor Item_{prefixo}"
        if col_total in df.columns:
            return pd.to_numeric(df[col_total], errors="coerce")
        if col_item in df.columns:
            return pd.to_numeric(df[col_item], errors="coerce")
        return pd.Series([0] * len(df), index=df.index)

    df_estoque["Valor Entrada"] = _obter_valor(df_estoque, "entrada").fillna(0)
    df_estoque["Valor Venda"] = _obter_valor(df_estoque, "saida").fillna(0)
    df_estoque["Lucro"] = df_estoque["Valor Venda"] - df_estoque["Valor Entrada"]

    # Adicionar colunas de mês para análise temporal
    if "Data Emissão_entrada" in df_estoque.columns:
        df_estoque["Mês Entrada"] = pd.to_datetime(
            df_estoque["Data Emissão_entrada"], errors="coerce"
        ).dt.to_period("M").dt.start_time
    if "Data Saída" in df_estoque.columns:
        df_estoque["Mês Saída"] = pd.to_datetime(
            df_estoque["Data Saída"], errors="coerce"
        ).dt.to_period("M").dt.start_time

    # Coluna base para filtros (prioriza data de saída para veículos vendidos)
    df_estoque["Data Base"] = df_estoque["Data Saída"].combine_first(
        df_estoque.get("Data Emissão_entrada")
    )
    df_estoque["Mês Base"] = pd.to_datetime(
        df_estoque["Data Base"], errors="coerce"
    ).dt.to_period("M").dt.start_time

    log.info("Estoque fiscal gerado com sucesso.")
    return df_estoque

def calcular_kpis_financeiros(df_estoque: pd.DataFrame) -> dict:
    """Calcula os principais KPIs financeiros e fiscais.

    Args:
        df_estoque (pd.DataFrame): DataFrame com o estoque fiscal.

    Returns:
        dict: Dicionário com os KPIs calculados.
    """
    log.info("Calculando KPIs financeiros.")

    # Garante que \'Situação\' exista, mesmo que o DataFrame esteja vazio
    if "Situação" not in df_estoque.columns:
        df_estoque["Situação"] = pd.NA

    vendidos = df_estoque[df_estoque["Situação"] == "Vendido"].copy() if "Situação" in df_estoque.columns else pd.DataFrame()
    estoque_atual = df_estoque[df_estoque["Situação"] == "Em Estoque"].copy() if "Situação" in df_estoque.columns else pd.DataFrame()

    total_vendido = vendidos["Valor Venda"].sum() if not vendidos.empty else 0
    lucro_bruto = vendidos["Lucro"].sum() if not vendidos.empty else 0

    # Calcular ICMS Débito e Crédito
    # Assumindo que ICMS Valor_saida é o ICMS a ser debitado (venda)
    # e ICMS Valor_entrada é o ICMS a ser creditado (compra)
    icms_debito = pd.to_numeric(vendidos.get("ICMS Valor_saida"), errors="coerce").fillna(0).sum() if not vendidos.empty else 0
    icms_credito = pd.to_numeric(vendidos.get("ICMS Valor_entrada"), errors="coerce").fillna(0).sum() if not vendidos.empty else 0

    lucro_liquido = lucro_bruto - (icms_debito - icms_credito)
    estoque_valor = estoque_atual["Valor Entrada"].sum() if not estoque_atual.empty else 0

    kpis = {
        "Total Vendido (R$)": f"R$ {total_vendido:,.2f}",
        "Lucro Líquido (R$)": f"R$ {lucro_liquido:,.2f}",
        "ICMS Débito (R$)": f"R$ {icms_debito:,.2f}",
        "ICMS Crédito (R$)": f"R$ {icms_credito:,.2f}",
        "ICMS Apurado (R$)": f"R$ {(icms_debito - icms_credito):,.2f}",
        "Estoque Atual (R$)": f"R$ {estoque_valor:,.2f}",
    }
    log.info("KPIs calculados: %s", kpis)
    return kpis

def gerar_relatorio_excel(df_consolidado: pd.DataFrame, kpis: dict, output_path: str):
    """Gera um arquivo Excel com as abas de Veículos Vendidos, Em Estoque, Resumo Mensal e Alertas.

    Args:
        df_consolidado (pd.DataFrame): DataFrame consolidado com todos os dados de veículos.
        kpis (dict): Dicionário com os KPIs calculados.
        output_path (str): Caminho completo para salvar o arquivo Excel.
    """
    log.info(f"Gerando relatório Excel em: {output_path}")

    vendidos = df_consolidado[df_consolidado["Situação"] == "Vendido"].copy() if "Situação" in df_consolidado.columns else pd.DataFrame()
    estoque = df_consolidado[df_consolidado["Situação"] == "Em Estoque"].copy() if "Situação" in df_consolidado.columns else pd.DataFrame()
    saida_sem_entrada = df_consolidado[df_consolidado["Situação"] == "Saída sem Entrada"].copy() if "Situação" in df_consolidado.columns else pd.DataFrame()

    # Gerar resumo mensal
    resumo_mensal = gerar_resumo_mensal(df_consolidado) if not df_consolidado.empty else pd.DataFrame()

    # Gerar alertas de auditoria
    alertas_auditoria = gerar_alertas_auditoria(df_consolidado[df_consolidado["Tipo Nota_entrada"] == "Entrada"] if "Tipo Nota_entrada" in df_consolidado.columns else pd.DataFrame(), df_consolidado[df_consolidado["Tipo Nota_saida"] == "Saída"] if "Tipo Nota_saida" in df_consolidado.columns else pd.DataFrame()) if not df_consolidado.empty else pd.DataFrame()

    try:
        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            if not vendidos.empty:
                vendidos.to_excel(writer, sheet_name="Veículos Vendidos", index=False)
                log.info("Aba \'Veículos Vendidos\' adicionada.")
            else:
                log.info("Nenhum veículo vendido para adicionar à aba.")

            if not estoque.empty:
                estoque.to_excel(writer, sheet_name="Veículos em Estoque", index=False)
                log.info("Aba \'Veículos em Estoque\' adicionada.")
            else:
                log.info("Nenhum veículo em estoque para adicionar à aba.")

            if not saida_sem_entrada.empty:
                saida_sem_entrada.to_excel(writer, sheet_name="Saídas sem Entrada", index=False)
                log.info("Aba \'Saídas sem Entrada\' adicionada.")
            else:
                log.info("Nenhuma saída sem entrada para adicionar à aba.")

            if not resumo_mensal.empty:
                resumo_mensal.to_excel(writer, sheet_name="Resumo Mensal", index=False)
                log.info("Aba \'Resumo Mensal\' adicionada.")
            else:
                log.info("Nenhum resumo mensal para adicionar à aba.")

            if not alertas_auditoria.empty:
                alertas_auditoria.to_excel(writer, sheet_name="Alertas Auditoria", index=False)
                log.info("Aba \'Alertas Auditoria\' adicionada.")
            else:
                log.info("Nenhum alerta de auditoria para adicionar à aba.")

            # Adicionar KPIs em uma aba separada ou no resumo
            kpis_df = pd.DataFrame([kpis]).T.reset_index()
            kpis_df.columns = ["KPI", "Valor"]
            kpis_df.to_excel(writer, sheet_name="KPIs", index=False)
            log.info("Aba \'KPIs\' adicionada.")

        log.info("Relatório Excel gerado com sucesso.")
    except Exception as e:
        log.error(f"Erro ao salvar o arquivo Excel: {e}")
        raise

def validar_dados_finais(df_consolidado: pd.DataFrame) -> bool:
    """Realiza validações finais nos dados consolidados.

    Args:
        df_consolidado (pd.DataFrame): DataFrame consolidado.

    Returns:
        bool: True se os dados forem válidos, False caso contrário.
    """
    log.info("Iniciando validação final dos dados.")
    erros_validacao = []

    # Exemplo de validação: Verificar se há chassi/placa duplicados em estoque
    estoque_df = df_consolidado[df_consolidado["Situação"] == "Em Estoque"] if "Situação" in df_consolidado.columns else pd.DataFrame()
    if not estoque_df.empty:
        duplicados_estoque = estoque_df[estoque_df.duplicated(subset=["Chave"], keep=False)]
        if not duplicados_estoque.empty:
            erros_validacao.append(f"Chassis/Placas duplicados em estoque: {duplicados_estoque['Chave'].tolist()}")
            log.warning(f"Validação: Chassis/Placas duplicados em estoque encontrados.")

    # Exemplo de validação: Verificar se há valores nulos em colunas críticas para veículos vendidos
    vendidos_df = df_consolidado[df_consolidado["Situação"] == "Vendido"] if "Situação" in df_consolidado.columns else pd.DataFrame()
    colunas_criticas = ["Chassi_entrada", "Valor Entrada", "Data Emissão_entrada", "Valor Venda", "Data Saída"]
    for col in colunas_criticas:
        if col in vendidos_df.columns and vendidos_df[col].isnull().any():
            erros_validacao.append(f"Valores nulos na coluna crítica {col} para veículos vendidos.")
            log.warning(f"Validação: Valores nulos na coluna {col} para veículos vendidos.")

    if erros_validacao:
        log.error("Falha na validação final dos dados:")
        for erro in erros_validacao:
            log.error(f"- {erro}")
        return False
    
    log.info("Validação final dos dados concluída com sucesso.")
    return True

def gerar_alertas_auditoria(df_entrada: pd.DataFrame, df_saida: pd.DataFrame) -> pd.DataFrame:
    """Gera alertas de auditoria com base em inconsistências entre entradas e saídas.

    Args:
        df_entrada (pd.DataFrame): DataFrame com as notas de entrada de veículos.
        df_saida (pd.DataFrame): DataFrame com as notas de saída de veículos.

    Returns:
        pd.DataFrame: DataFrame com os alertas de auditoria.
    """
    log.info("Iniciando geração de alertas de auditoria.")
    erros = []

    # Alertas de duplicidade
    duplicadas_entrada = df_entrada[df_entrada.duplicated("Chave", keep=False)] if "Chave" in df_entrada.columns else pd.DataFrame()
    for _, row in duplicadas_entrada.iterrows():
        erros.append({"Tipo": "Entrada", "Chave": row.get("Chave"), "Erro": "DUPLICIDADE_ENTRADA", "XML Path": row.get("XML Path_entrada")})

    duplicadas_saida = df_saida[df_saida.duplicated("Chave", keep=False)] if "Chave" in df_saida.columns else pd.DataFrame()
    for _, row in duplicadas_saida.iterrows():
        erros.append({"Tipo": "Saída", "Chave": row.get("Chave"), "Erro": "DUPLICIDADE_SAIDA", "XML Path": row.get("XML Path_saida")})

    # Saída sem correspondente na entrada (já tratado na função gerar_estoque_fiscal com \'Saída sem Entrada\')
    # Esta função de alertas pode focar em outras inconsistências ou detalhar mais as já existentes

    if not erros:
        log.info("Nenhum alerta de auditoria encontrado.")
        return pd.DataFrame(columns=["Tipo", "Chave", "Erro", "XML Path"])

    log.warning(f"Alertas de auditoria gerados: {len(erros)} inconsistências encontradas.")
    return pd.DataFrame(erros)

def gerar_resumo_mensal(df_estoque: pd.DataFrame) -> pd.DataFrame:
    """Gera um resumo financeiro mensal com lucros e ICMS.

    Args:
        df_estoque (pd.DataFrame): DataFrame com o estoque fiscal.

    Returns:
        pd.DataFrame: DataFrame com o resumo financeiro mensal.
    """
    log.info("Gerando resumo financeiro mensal.")

    df = df_estoque.copy()
    
    # Garante que as colunas de data sejam datetime
    df["Mês Base"] = pd.to_datetime(df["Mês Base"], errors=\'coerce\')
    df["Mês Entrada"] = pd.to_datetime(df["Mês Entrada"], errors=\'coerce\')
    df["Mês Saída"] = pd.to_datetime(df["Mês Saída"], errors=\'coerce\')

    # Preencher Mês Resumo com a data mais relevante
    df["Mês Resumo"] = df["Mês Saída"].combine_first(df["Mês Entrada"])
    df = df.dropna(subset=["Mês Resumo"]) # Remover linhas sem data de referência

    # Converter para formato de período para agrupamento
    df["Mês Resumo"] = df["Mês Resumo"].dt.to_period("M")

    group_cols = ["Mês Resumo"]
    if "Emitente CNPJ/CPF_entrada" in df.columns: # Usar o CNPJ do emitente da entrada como referência da empresa
        df["Empresa CNPJ"] = df["Emitente CNPJ/CPF_entrada"]
        group_cols.insert(0, "Empresa CNPJ")

    # Calcular ICMS Débito e Crédito para o resumo
    df["ICMS Débito"] = pd.to_numeric(df.get("ICMS Valor_saida"), errors="coerce").fillna(0)
    df["ICMS Crédito"] = pd.to_numeric(df.get("ICMS Valor_entrada"), errors="coerce").fillna(0)

    resumo = (
        df.groupby(group_cols)
        .agg(
            {
                "Valor Entrada": "sum",
                "Valor Venda": "sum",
                "Lucro": "sum",
                "ICMS Débito": "sum",
                "ICMS Crédito": "sum",
            }
        )
        .reset_index()
        .rename(
            columns={
                "Mês Resumo": "Mês",
                "Valor Entrada": "Total Entradas",
                "Valor Venda": "Total Saídas",
                "Lucro": "Lucro Bruto",
            }
        )
    )

    resumo["Lucro Líquido"] = resumo["Lucro Bruto"] - (
        resumo["ICMS Débito"] - resumo["ICMS Crédito"]
    )

    # Calcular Saldo Estoque (valor dos veículos que ainda estão em estoque no final do mês)
    # Isso é mais complexo e pode exigir uma abordagem de snapshot mensal ou um cálculo cumulativo.
    # Por simplicidade, aqui vamos somar o valor de entrada dos veículos \'Em Estoque\' para o mês de referência.
    # Uma abordagem mais robusta exigiria considerar o estoque acumulado.
    estoque_vals = (
        df[df["Situação"] == "Em Estoque"]
        .groupby(group_cols)["Valor Entrada"]
        .sum()
    )
    resumo["Saldo Estoque"] = resumo.apply(lambda row: estoque_vals.get(tuple(row[col] for col in group_cols), 0), axis=1)

    # Converter a coluna \'Mês\' de volta para o início do mês para melhor visualização
    resumo["Mês"] = resumo["Mês"].dt.to_timestamp()

    log.info("Resumo financeiro mensal gerado com sucesso.")
    return resumo




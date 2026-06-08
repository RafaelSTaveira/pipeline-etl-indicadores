"""Transformação: Bronze (bruto) -> Silver (limpo e normalizado) -> Gold (agregado mensal, pronto para BI)."""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config import INDICADORES

logger = logging.getLogger(__name__)

DDL_SILVER = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.indicadores (
    data_referencia DATE NOT NULL,
    codigo_indicador VARCHAR(40) NOT NULL,
    nome_indicador VARCHAR(120) NOT NULL,
    valor NUMERIC NOT NULL,
    unidade VARCHAR(20) NOT NULL,
    granularidade VARCHAR(20) NOT NULL,
    PRIMARY KEY (data_referencia, codigo_indicador)
);
"""

DDL_GOLD = """
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.indicadores_mensais (
    mes_referencia DATE PRIMARY KEY,
    cambio_usd_brl NUMERIC,
    selic_mensal NUMERIC,
    ipca_variacao NUMERIC,
    taxa_desemprego NUMERIC
);
"""

CATALOGO_POR_CODIGO = {indicador.codigo: indicador for indicador in INDICADORES}


def _normalizar_para_silver() -> str:
    """Monta o SELECT que lê a Bronze, tipa os valores e agrega séries diárias para a granularidade mensal."""
    casos_nome = " ".join(
        f"WHEN '{ind.codigo}' THEN '{ind.nome}'" for ind in INDICADORES
    )
    casos_unidade = " ".join(
        f"WHEN '{ind.codigo}' THEN '{ind.unidade}'" for ind in INDICADORES
    )
    codigos_diarios = ", ".join(f"'{ind.codigo}'" for ind in INDICADORES if ind.granularidade == "diaria")
    codigos_mensais = ", ".join(f"'{ind.codigo}'" for ind in INDICADORES if ind.granularidade == "mensal")

    return f"""
    WITH bruto AS (
        SELECT
            codigo_indicador,
            data_referencia,
            valor_raw::NUMERIC AS valor
        FROM bronze.indicadores_raw
    ),
    -- séries diárias (câmbio, Selic) são agregadas para média mensal
    diarias AS (
        SELECT
            codigo_indicador,
            date_trunc('month', data_referencia)::DATE AS data_referencia,
            AVG(valor) AS valor
        FROM bruto
        WHERE codigo_indicador IN ({codigos_diarios})
        GROUP BY 1, 2
    ),
    -- séries mensais já vêm na granularidade alvo; normalizamos a data para o 1º dia do mês
    mensais AS (
        SELECT
            codigo_indicador,
            date_trunc('month', data_referencia)::DATE AS data_referencia,
            valor
        FROM bruto
        WHERE codigo_indicador IN ({codigos_mensais})
    ),
    unificado AS (
        SELECT * FROM diarias
        UNION ALL
        SELECT * FROM mensais
    )
    SELECT
        data_referencia,
        codigo_indicador,
        CASE codigo_indicador {casos_nome} END AS nome_indicador,
        valor,
        CASE codigo_indicador {casos_unidade} END AS unidade,
        CASE WHEN codigo_indicador IN ({codigos_diarios}) THEN 'mensal (média)' ELSE 'mensal' END AS granularidade
    FROM unificado
    """


def transformar_silver(engine: Engine) -> int:
    """Lê a Bronze, normaliza/agrega para granularidade mensal e grava a Silver (full refresh)."""
    with engine.begin() as conexao:
        conexao.execute(text(DDL_SILVER))
        conexao.execute(text("TRUNCATE TABLE silver.indicadores"))
        resultado = conexao.execute(text(f"""
            INSERT INTO silver.indicadores
            (data_referencia, codigo_indicador, nome_indicador, valor, unidade, granularidade)
            {_normalizar_para_silver()}
        """))
    logger.info("Silver atualizada com %d linhas", resultado.rowcount)
    return resultado.rowcount


def construir_gold(engine: Engine) -> int:
    """Pivota a Silver (formato longo) para a Gold (formato largo, uma linha por mês) — pronta para BI."""
    with engine.begin() as conexao:
        conexao.execute(text(DDL_GOLD))
        conexao.execute(text("TRUNCATE TABLE gold.indicadores_mensais"))
        resultado = conexao.execute(text("""
            INSERT INTO gold.indicadores_mensais
            (mes_referencia, cambio_usd_brl, selic_mensal, ipca_variacao, taxa_desemprego)
            SELECT
                data_referencia AS mes_referencia,
                MAX(valor) FILTER (WHERE codigo_indicador = 'cambio_usd_brl')   AS cambio_usd_brl,
                MAX(valor) FILTER (WHERE codigo_indicador = 'selic_diaria')     AS selic_mensal,
                MAX(valor) FILTER (WHERE codigo_indicador = 'ipca_variacao')    AS ipca_variacao,
                MAX(valor) FILTER (WHERE codigo_indicador = 'taxa_desemprego')  AS taxa_desemprego
            FROM silver.indicadores
            GROUP BY data_referencia
            ORDER BY data_referencia
        """))
    logger.info("Gold reconstruída com %d meses", resultado.rowcount)
    return resultado.rowcount

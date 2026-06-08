"""Extração: busca os indicadores na API do Banco Central e grava na camada Bronze (dados brutos)."""

import logging
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config import INDICADORES, url_serie_bcb

logger = logging.getLogger(__name__)

DDL_BRONZE = """
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.indicadores_raw (
    id BIGSERIAL PRIMARY KEY,
    codigo_serie_bcb INTEGER NOT NULL,
    codigo_indicador VARCHAR(40) NOT NULL,
    data_referencia DATE NOT NULL,
    valor_raw TEXT NOT NULL,
    extraido_em TIMESTAMP NOT NULL,
    UNIQUE (codigo_indicador, data_referencia)
);
"""


def _buscar_serie(codigo_serie: int) -> list[dict]:
    resposta = requests.get(url_serie_bcb(codigo_serie), timeout=60)
    resposta.raise_for_status()
    return resposta.json()


def extrair_indicadores(engine: Engine) -> int:
    """Busca todos os indicadores do catálogo na API do BCB e grava os registros brutos na Bronze.

    Retorna a quantidade total de linhas inseridas.
    """
    with engine.begin() as conexao:
        conexao.execute(text(DDL_BRONZE))

    momento_extracao = datetime.now(timezone.utc).replace(tzinfo=None)
    total_inserido = 0

    for indicador in INDICADORES:
        registros = _buscar_serie(indicador.codigo_serie_bcb)
        if not registros:
            logger.warning("Nenhum dado retornado para %s (série %s)", indicador.codigo, indicador.codigo_serie_bcb)
            continue

        df = pd.DataFrame(registros)
        df["codigo_serie_bcb"] = indicador.codigo_serie_bcb
        df["codigo_indicador"] = indicador.codigo
        df["data_referencia"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date
        df["valor_raw"] = df["valor"]
        df["extraido_em"] = momento_extracao
        df = df[["codigo_serie_bcb", "codigo_indicador", "data_referencia", "valor_raw", "extraido_em"]]

        with engine.begin() as conexao:
            conexao.execute(text("DELETE FROM bronze.indicadores_raw WHERE codigo_indicador = :codigo"), {"codigo": indicador.codigo})
            df.to_sql("indicadores_raw", conexao, schema="bronze", if_exists="append", index=False)

        logger.info("[%s] %d registros gravados na Bronze", indicador.codigo, len(df))
        total_inserido += len(df)

    return total_inserido

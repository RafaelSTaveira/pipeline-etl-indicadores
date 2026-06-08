"""Orquestração do pipeline ETL: extract -> transform (silver) -> transform (gold).

Reaproveitado tanto pela execução standalone (`python -m src.pipeline`) quanto pela DAG do Airflow,
para que a lógica de negócio fique em um único lugar e a orquestração seja apenas "encanamento".
"""

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.config import url_conexao_dw
from src.extract import extrair_indicadores
from src.transform import construir_gold, transformar_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def obter_engine() -> Engine:
    return create_engine(url_conexao_dw())


def etapa_extract(engine: Engine | None = None) -> int:
    engine = engine or obter_engine()
    linhas = extrair_indicadores(engine)
    logger.info("EXTRACT concluído: %d linhas brutas gravadas na Bronze", linhas)
    return linhas


def etapa_transform_silver(engine: Engine | None = None) -> int:
    engine = engine or obter_engine()
    linhas = transformar_silver(engine)
    logger.info("TRANSFORM (silver) concluído: %d linhas normalizadas", linhas)
    return linhas


def etapa_transform_gold(engine: Engine | None = None) -> int:
    engine = engine or obter_engine()
    linhas = construir_gold(engine)
    logger.info("TRANSFORM (gold) concluído: %d meses agregados", linhas)
    return linhas


def executar_pipeline_completo() -> None:
    engine = obter_engine()
    etapa_extract(engine)
    etapa_transform_silver(engine)
    etapa_transform_gold(engine)
    logger.info("Pipeline ETL concluído com sucesso (Bronze -> Silver -> Gold).")


if __name__ == "__main__":
    executar_pipeline_completo()

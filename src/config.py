"""Configurações centrais do pipeline: conexão com o data warehouse e catálogo de indicadores."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Indicador:
    codigo_serie_bcb: int
    codigo: str
    nome: str
    unidade: str
    granularidade: str  # "diaria" ou "mensal"


# Catálogo de indicadores extraídos da API de Séries Temporais do Banco Central (SGS).
# Documentação: https://dadosabertos.bcb.gov.br/dataset/2-pre-selecionadas-series-mais-consultadas
INDICADORES = [
    Indicador(1, "cambio_usd_brl", "Câmbio USD/BRL (compra)", "R$", "diaria"),
    Indicador(11, "selic_diaria", "Taxa Selic (diária)", "% a.d.", "diaria"),
    Indicador(433, "ipca_variacao", "IPCA — variação mensal", "%", "mensal"),
    Indicador(24369, "taxa_desemprego", "Taxa de desemprego (PNAD Contínua)", "%", "mensal"),
]

DATA_INICIAL_EXTRACAO = "01/01/2018"


def url_serie_bcb(codigo_serie: int, data_inicial: str = DATA_INICIAL_EXTRACAO) -> str:
    return (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados"
        f"?formato=json&dataInicial={data_inicial}"
    )


def url_conexao_dw() -> str:
    host = os.getenv("DW_HOST", "localhost")
    port = os.getenv("DW_PORT", "5434")
    nome = os.getenv("DW_NAME", "indicadores")
    usuario = os.getenv("DW_USER", "etl_user")
    senha = os.getenv("DW_PASSWORD", "etl_pass")
    return f"postgresql+psycopg2://{usuario}:{senha}@{host}:{port}/{nome}"

"""Visualização final do pipeline: dashboard Streamlit consultando a camada Gold do data warehouse."""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

from src.config import url_conexao_dw

st.set_page_config(page_title="Indicadores Econômicos — Data Warehouse", page_icon="🏗️", layout="wide")


@st.cache_resource
def obter_engine():
    return create_engine(url_conexao_dw())


@st.cache_data(ttl=300)
def carregar_gold() -> pd.DataFrame:
    engine = obter_engine()
    df = pd.read_sql(
        "SELECT * FROM gold.indicadores_mensais ORDER BY mes_referencia",
        engine,
        parse_dates=["mes_referencia"],
    )
    return df.set_index("mes_referencia")


st.title("🏗️ Pipeline ETL — Indicadores Econômicos do Brasil")
st.caption(
    "Camada **Gold** do data warehouse, alimentada por um pipeline ETL orquestrado com Airflow: "
    "extração da API do Banco Central → limpeza/normalização (Silver) → agregação mensal (Gold)."
)

try:
    gold = carregar_gold()
except Exception as erro:
    st.error(
        "Não foi possível conectar ao data warehouse. Verifique se o pipeline já foi executado "
        "(`python -m src.pipeline` ou a DAG no Airflow) e se o Postgres está no ar.\n\n"
        f"Detalhe técnico: {erro}"
    )
    st.stop()

if gold.empty:
    st.warning("A camada Gold está vazia. Rode o pipeline ETL primeiro (`python -m src.pipeline`).")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
ultimo = gold.iloc[-1]
col1.metric("Câmbio USD/BRL (médio do mês)", f"R$ {ultimo['cambio_usd_brl']:.2f}")
col2.metric("Selic (taxa diária média do mês)", f"{ultimo['selic_mensal'] * 100:.3f}%")
col3.metric("IPCA (variação mensal)", f"{ultimo['ipca_variacao']:.2f}%")
col4.metric("Taxa de desemprego", f"{ultimo['taxa_desemprego']:.1f}%")
st.caption(f"Referência: {gold.index[-1].strftime('%m/%Y')} — {len(gold)} meses carregados na Gold")

st.markdown("---")

indicador_legenda = {
    "cambio_usd_brl": ("Câmbio USD/BRL", "R$"),
    "selic_mensal": ("Selic (taxa diária média do mês)", "%"),
    "ipca_variacao": ("IPCA — variação mensal", "%"),
    "taxa_desemprego": ("Taxa de desemprego", "%"),
}

col_esq, col_dir = st.columns(2)
for i, (coluna, (titulo, unidade)) in enumerate(indicador_legenda.items()):
    alvo = col_esq if i % 2 == 0 else col_dir
    with alvo:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=gold.index, y=gold[coluna], mode="lines", name=titulo))
        fig.update_layout(title=f"{titulo} ({unidade})", height=320, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.subheader("Tabela — camada Gold (`gold.indicadores_mensais`)")
st.dataframe(gold.round(4), use_container_width=True)

st.caption(
    "Arquitetura: Bronze (dados brutos da API) → Silver (limpos, tipados, normalizados para "
    "granularidade mensal) → Gold (agregados, prontos para consumo por BI). "
    "Pipeline orquestrado via Airflow, executando diariamente."
)

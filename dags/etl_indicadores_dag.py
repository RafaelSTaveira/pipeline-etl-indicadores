"""DAG do Airflow: orquestra o pipeline ETL de indicadores econômicos (Bronze -> Silver -> Gold).

A lógica de negócio mora em `src/` (extract, transform, pipeline) e é reaproveitada aqui —
a DAG é responsável apenas por agendamento, dependências entre tarefas, retries e logging,
mantendo o código testável fora do Airflow (ver `python -m src.pipeline`).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.pipeline import etapa_extract, etapa_transform_gold, etapa_transform_silver

ARGUMENTOS_PADRAO = {
    "owner": "rafael-taveira",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_indicadores_economicos",
    description="Extrai indicadores econômicos do Banco Central, normaliza e agrega em um data warehouse PostgreSQL",
    default_args=ARGUMENTOS_PADRAO,
    schedule="0 9 * * *",  # diariamente às 09h — após o fechamento das séries do dia anterior
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "indicadores", "bcb", "portfolio"],
) as dag:

    extract = PythonOperator(
        task_id="extract_bronze",
        python_callable=etapa_extract,
        doc_md="Busca os indicadores na API do Banco Central (SGS) e grava os dados brutos na camada Bronze.",
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=etapa_transform_silver,
        doc_md="Limpa, tipa e normaliza os dados da Bronze (incluindo agregação de séries diárias para mensal) na camada Silver.",
    )

    transform_gold = PythonOperator(
        task_id="transform_gold",
        python_callable=etapa_transform_gold,
        doc_md="Pivota a Silver (formato longo) em uma tabela mensal larga na camada Gold, pronta para consumo por BI/dashboards.",
    )

    extract >> transform_silver >> transform_gold

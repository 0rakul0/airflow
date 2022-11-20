from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from baixa_csv import cluster
from trata_csv import trata

with DAG('tratamento_fii_rank', start_date = datetime(2022,11,20),
         schedule_interval="30 * * * *", catchup=False, tags=['fundos_fii','Jefferson'],
         template_searchpath = '/opt/airflow/sql') as dag:
    # extração
    def baixa_csv():
        exec = cluster()
        exec.dataframe()

    def trata_csv():
        trata = trata()
        trata.formata

    # chamada da dag da extração
    baixa_csv = PythonOperator(
        task_id='baixa_csv',
        python_callable = baixa_csv
    )

    trata_csv = PythonOperator(
        task_id='trata_csv',
        python_callable = trata_csv
    )

    criar_tabela_fii = PostgresOperator(
        task_id='criar_tabela_fii',
        postgres_conn_id='postgres-airflow',
        sql='fundos_fii_csv.sql'
    )

    baixa_csv >> trata_csv
    criar_tabela_fii
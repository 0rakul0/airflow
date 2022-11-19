from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

# o nome do arquivo, start_date=datetime a data de hoje, schedule_interval=quando vai ser executado (crontab)
with DAG('dg_exec_sql', start_date = datetime(2022,11,19),
         schedule_interval="30 * * * *",
         catchup=False,
         template_searchpath = '/opt/airflow/sql') as dag:
    criar_tabela_db = PostgresOperator(
        task_id='criar_tabela_db',
        postgres_conn_id = 'postgres-airflow',
        sql = 'criar_tabela_db.sql'
    )

    insere_dados_tabela_db = PostgresOperator(
        task_id='insere_dados_tabela_db.sql',
        postgres_conn_id='postgres-airflow',
        sql = 'insere_dados_tabela_db.sql'
    )

criar_tabela_db >> insere_dados_tabela_db
from datetime import datetime
from airflow import DAG



import sys
sys.path.append('./HT10')
import json
import os


from HT10.my_postgres_operators import myPostgresOperator_TablesToCSV
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="HT10_dag_db",
    description="Hometask of 10th lesson DB Load",
    start_date = datetime(2021,10,24,12,00),
    schedule_interval = '@once',
    )

dummy1 = DummyOperator(task_id="start_dag", dag=dag)
dummy2 = DummyOperator(task_id="end_dag", dag=dag)
tables=['clients','orders','products','aisles','departments']
tables_dags=[]

for tbl in tables:
    tables_dags.append( myPostgresOperator_TablesToCSV(
                       task_id = f"save_table_{tbl}",
                       dag = dag,
                       postgres_conn_id="Postgres_HT10",
                       database="dshop",
                       folder=os.path.join('/','bronze','HT10_download'),
                       table=tbl,
                       sql=""
                       
                       )
            )
    

dummy1 >> tables_dags >> dummy2
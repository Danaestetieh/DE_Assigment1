
import time
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import psycopg2
from sqlalchemy import create_engine,Table, Column, Integer, String, MetaData,Date
import pandas as pd

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='DanaEstetieh',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['Q2DE'],
) as dag:

    task1 = BashOperator(
        task_id='ExtractCSV',
        bash_command="pip install pymongo",
    )

    def load_CSVfile(**kwargs):
        host="de_postgres" 
        database="psql_data_environment"
        user="psql_user"
        password="psql"
        port='5432'
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        tablename="client_list"
        rdf = pd.read_sql_table(
            tablename,
            con=engine
        )
        import json
        jsonfile=rdf.to_json(orient='records')
        with open('data.json', 'w') as file:
            json.dump(jsonfile, file)
        return 'Success'

    task2 = PythonOperator(
        task_id='load_CSVfile',
        python_callable=load_CSVfile,
    )
    def load_mongodb(**kwargs):
        from pymongo import MongoClient
        import json

        client = MongoClient("mongodb://mongopsql:mongo@de_mongo:27017")

        db = client["DE"]

        Collection = db["dataset"]

        with open('data.json') as file:
            file_data = json.load(file)
            listfile=json.loads(file_data)
            Collection.insert_many(listfile)

        return 'Success'

    task3 = PythonOperator(
        task_id='load_mongodb',
        python_callable=load_mongodb,
    )
    task1 >> task2 
    task2 >> task3
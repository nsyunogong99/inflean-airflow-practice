import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'ORANGE', 'BANANA']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])
    
    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit
    )

    py_t1
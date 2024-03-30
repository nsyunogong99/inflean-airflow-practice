import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG

with DAG(
    dag_id="dags_python_show_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 3, 10, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id='python_task')
    def show_tempalte(**kwargs):
        from pprint import pprint
        pprint(kwargs)
    
    show_tempalte()
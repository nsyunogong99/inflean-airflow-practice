import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_branch_decorator",
    schedule=None,
    start_date=datetime(2024,3,31, tz='Asia/Seoul'),
    catchup=False,
) as dag:
    
    bash_upstream_1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!')
    
    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('정상 처리')

    @task(task_id='python_downstream_1', trigger_rul='all_done')
    def python_downstream_1():
        print('정상 처리')

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    
    t1_orange = BashOperator(
        task_id="t1_orange"
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE"
    )

    t2_avocado = BashOperator(
        task_id="t2_avocado"
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO"
    )

    t1_orange >> t2_avocado
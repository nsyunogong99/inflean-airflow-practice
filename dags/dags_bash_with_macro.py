import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG

with DAG(
    dag_id="dags_bash_with_macro",
    schedule="10 0 * * 6#2", # 매월 둘째주 토요일 
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    # START_DATE: 전월 말일, END_DATE: 1일 전
    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        env={'START_DATE': '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
             'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}'
            },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )

    # START_DATE: 2주전 월요일, END_DATE: 2주전 토요일
    bash_task_2 = BashOperator(
        task_id='bash_task_2',
        env={'START_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") macros.dateutil.relativedelta.relativedelta(days=19)) | ds }}',
             'END_DATE': '{{ (data_interval_start.in_timezone("Asia/Seoul") macros.dateutil.relativedelta.relativedelta(days=14)) | ds }}'
            },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )
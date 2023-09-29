from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'Tiago',
    'depends_on_past':False,
    'start_date': datetime(2023,1,1)
} 

dag = DAG(
    "exercicio_01",
    description = "Primeiro exercicio com o AirFlow",
    default_args = default_args,
    schadule_interval = "*/5 * * * *",
    tags = ['exercicio'],
    catchup = False
)

def hello_world():
    print("Hello world do AirFlow!!!!")

hello_python = PythonOperator(
    task_id = "hello_do_python",
    python_callable = hello_world,
    dag=dag
)

hello_bash = BashOperator(
    task_id="hello_do_bash",
    bash_command="echo 'Hello World do Bash'",
    dag=dag
)

hello_python >> hello_bash
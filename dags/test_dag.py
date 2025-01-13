from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Configuration that differs between dev and prod
CONFIGS = {
    'dev': {
        'email': ['paruchurivijay40@duck.com'],
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
}

# Choose config based on environment
env = 'dev'  # Change this to 'dev' for local testing
config = CONFIGS[env]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': config['email'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': config['retries'],
    'retry_delay': config['retry_delay'],
}

def your_python_function(**context):
    """
    Example Python function for DAG task
    """
    print("Executing task! Test")
    # Your code here
    return "Task completed"

# Test function that can be run locally
def test_your_python_function():
    result = your_python_function()
    assert result == "Task completed"
    print("Test passed!")

with DAG(
    'example_dag12',
    default_args=default_args,
    description='An example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = PythonOperator(
        task_id='python_task',
        python_callable=your_python_function,
    )

    t2 = BashOperator(
        task_id='bash_task',
        bash_command='echo "Hello from Bash!"',
    )

    t1 >> t2  # Set task dependency

if __name__ == "__main__":
    # This will only run when file is run directly
    test_your_python_function()
    print("DAG file validated successfully!")

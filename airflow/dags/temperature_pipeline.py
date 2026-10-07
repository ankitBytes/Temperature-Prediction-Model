from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="temperature_prediction_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    training = BashOperator(
        task_id="training",
        bash_command=(
            "cd /home/vvdn/Desktop/Projects/Python/"
            "Temperature-prediction-model-using-pyspark && "
            "python scripts/run_training.py"
        ),
    )

    inference = BashOperator(
        task_id="inference",
        bash_command=(
            "cd /home/vvdn/Desktop/Projects/Python/"
            "Temperature-prediction-model-using-pyspark && "
            "python scripts/run_inference.py"
        ),
    )

    monitoring = BashOperator(
        task_id="monitoring",
        bash_command=(
            "cd /home/vvdn/Desktop/Projects/Python/"
            "Temperature-prediction-model-using-pyspark && "
            "python scripts/run_monitoring.py"
        ),
    )

    training >> inference >> monitoring
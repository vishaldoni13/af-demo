from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta, timezone
from airflow.decorators import task
from airflow.models import Variable
import os

# os.environ['AWS_ACCESS_KEY_ID'] = Variable.get("AWS_ACCESS_KEY_ID")
# os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get("AWS_SECRET_ACCESS_KEY")

#DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 16, 0, 0, tzinfo=timezone.utc),  # Adjusted for 12 AM UTC
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'AF_Ingest_Dag',
    default_args=default_args,
    description='Run container with KubernetesPodOperator',
    schedule='@once',  
    catchup=False,
)
# KubernetesPodOperator arguments
namespace = 'default'
in_cluster = False # Change to True if using same cluster as Airflow

# Volume and volume mount as we want to store processed data in persistent volume
volume_mount = k8s.V1VolumeMount(name='test', mount_path='/tmp')
volume = k8s.V1Volume(
        name='test',
        host_path=k8s.V1HostPathVolumeSource(path='/Users/ksarma/af-demo/tmp')
        )
with dag:

    @task
    def start_dag():
        """dummy start task"""
        return "start_dag"
    
    run_data_ingest = KubernetesPodOperator(
        namespace=namespace,
        cluster_context='rancher-desktop',
        config_file='~/.kube/config',
        name='data-ingest-pod',
        task_id='data-ingest-task',
        image_pull_policy='IfNotPresent',
        image='af-demo:data-ingest',  # Ensure this image is built and available
        volumes=[volume], 
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        in_cluster=in_cluster,
        env_vars={
            # "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            # "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
            "APP_ENV": "development", # Example of another non-secret env var
            "LOG_LEVEL": "INFO"
        },
        cmds=["python", "main.py"], 
        startup_timeout_seconds=600, 
        get_logs=True,
        annotations={"kubernetes.io/hostname": "af"}, #optional for labelling pods
    )

    @task
    def end_dag():
        """dummy end task, or flag for other dags to trigger off of"""
        print("end_dag")
        return "end_dag"
    
    # Setting up dependencies to run tasks sequentially
    start_dag() >> run_data_ingest >> end_dag() 
    # start_dag() >> end_dag() 
# from airflow import DAG
# from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator
from datetime import datetime
import os
import yaml


def create_dag(dag_id: str, schedule: str, default_args: dict, args: dict):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        t1 = GKEStartPodOperator(
            task_id='start_pod',
            project_id='kirsch-data-platform',
            location='us-central1',
            cluster_name='us-central1-kirsch-data-pla-dd54cf32-gke',
            namespace='default',
            image='us-central1-docker.pkg.dev/kirsch-data-platform/docker-repository/add-hc-pricing-data:95fdb60',
            cmds=['python', 'main.py', '--company', 'humana', '--file_url', 'https://developers.humana.com/Resource/DownloadPCTFile?fileType=innetwork&fileName=2022-08-25_1_in-network-rates_000000000000.csv.gz', '--file_name', 'test_2022-08-25_1_in-network-rates_000000000000.csv.gz']
        )
        t2 = GKEStartPodOperator(
            task_id='start_pod',
            project_id='kirsch-data-platform',
            location='us-central1', 
            cluster_name='us-central1-kirsch-data-pla-dd54cf32-gke',
            namespace='default',
            image='us-central1-docker.pkg.dev/kirsch-data-platform/docker-repository/add-hc-pricing-data:latest',
            name='add-hc-pricing-data',
            cmds=['python', 'main.py', '--company', 'humana', '--file_url', 'https://developers.humana.com/Resource/DownloadPCTFile?fileType=innetwork&fileName=2022-08-25_1_in-network-rates_000000000000.csv.gz', '--file_name', 'test_2022-08-25_1_in-network-rates_000000000000.csv.gz']
        )
        t3 = GKEStartPodOperator(
            task_id='start_pod',
            project_id='kirsch-data-platform',
            location='us-central1',
            cluster_name='us-central1-kirsch-data-pla-dd54cf32-gke',
            namespace='default',
            image='us-central1-docker.pkg.dev/kirsch-data-platform/docker-repository/add-hc-pricing-data:latest',
            name='add-hc-pricing-data',
            cmds=['python', 'main.py', '--company', 'humana', '--file_url', 'https://developers.humana.com/Resource/DownloadPCTFile?fileType=innetwork&fileName=2022-08-25_1_in-network-rates_000000000000.csv.gz', '--file_name', 'test_2022-08-25_1_in-network-rates_000000000000.csv.gz']
        )
        t1 >> t2
        t2 >> t3

    return dag

folder = 'dags/configs'
# build a dag for each number in range(10)
for dag_file in os.listdir(folder):

    dag_id = dag_file.split('.')[0]

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2022, 9, 26),
                    'catchup': False}
    with open(f"{folder}/{dag_file}", 'r') as stream:
        dag_config = yaml.safe_load(stream)
        schedule = dag_config['schedule']


        globals()[dag_id] = create_dag(dag_id,
                                    schedule,
                                    default_args)
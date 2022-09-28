from airflow import DAG
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator
from datetime import datetime
import os


def create_dag(dag_id,
               schedule,
               default_args):

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
            name='add-hc-pricing-data',
            cmds=['python', 'main.py', '--company', 'humana', '--file_url', 'https://developers.humana.com/Resource/DownloadPCTFile?fileType=innetwork&fileName=2022-08-25_1_in-network-rates_000000000000.csv.gz', '--file_name', 'test_2022-08-25_1_in-network-rates_000000000000.csv.gz']
        )

    return dag


# build a dag for each number in range(10)
for n in range(1):
    dag_id = 'add_hc_pricing'

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2022, 9, 26),
                    'catchup': False}

    schedule = '@daily'

    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  default_args)
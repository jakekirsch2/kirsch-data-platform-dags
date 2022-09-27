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
            cluster_name='kirsch-data-pla-dd54cf32-gke',
            namespace='default',
            image='us-central1-docker.pkg.dev/kirsch-data-platform/docker-repository/add-hc-pricing-data:984e509',
            name='add-hc-pricing-data',
            cmds=['python', 'main.py', '--company', 'uhc', '--file_url', 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Bind-Benefits--Inc-_TPA_UNITEDHEALTHCARE-CHOICE-PLUS_UCQ_in-network-rates.json.gz', '--file_name', '2022-09-01_Bind-Benefits--Inc-_TPA_UNITEDHEALTHCARE-CHOICE-PLUS_UCQ_in-network-rates.json.gz']
        )

    return dag


# build a dag for each number in range(10)
for n in range(1):
    dag_id = 'add_hc_pricing'

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2021, 1, 1),
                    'catchup': False}

    schedule = '@daily'

    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  default_args)
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from docker.types import Mount, DeviceRequest
import os

images_folder = Mount(
    target="/media/WebServerBackup2TB/PresaValledeBravo",
    source="/media/WebServerBackup2TB/data/geotiffs/landsat/PresaValledeBravo",
    type="bind"
)
secchi_prediction = Mount(
    target="/media/secchi_predictions/",
    source="/media/WebServerBackup2TB/secchi_predictions",
    type="bind"
)
pixel_values = Mount(
    target="/media/pixel_values",
    source="/media/WebServerBackup2TB/pixel_values",
    type="bind"
)
kalman_filter_results = Mount(
    target="/media/results",
    source="/media/WebServerBackup2TB/data/water/csv-files",
    type="bind"
)
kalman_filter_parameters = Mount(
    target="/media/kalman_filter",
    source="/media/WebServerBackup2TB/kalman_filter",
    type="bind"
)
water_masks = Mount(
    target="/media/WebServerBackup2TB/data/water-masks",
    source="/media/WebServerBackup2TB/data/water/water-masks",
    type="bind"
)

docker_socket="unix://var/run/docker.sock"
gitlab_connection = "gitlab_registry"

with DAG(
	"water_quality_dag",
	start_date=datetime(2005,10,28),
	schedule_interval="0 0 1,16 * *",
	catchup=True,
    concurrency=1,
    max_active_runs=1,
    render_template_as_native_obj=True
) as dag:
    pixel_value_extractor = DockerOperator(
        task_id="pixel_value_extractor",
        image="registry.gitlab.com/cicata/water_quality_prediction/pixel_value_extractor:1.1.0",
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_url=docker_socket,
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[images_folder,pixel_values]
    ) 
    secchi_predictor = DockerOperator(
        task_id="secchi_predictor",
        image="registry.gitlab.com/cicata/water_quality_prediction/predictor:1.1.1",
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_url=docker_socket,
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        device_requests=[DeviceRequest(count=-1,capabilities=[["gpu"]])],
        mounts=[pixel_values, secchi_prediction]
    )
    kalman_filter = DockerOperator(
        task_id="kalman_filter",
        image="registry.gitlab.com/cicata/water_quality_prediction/kalman-filter:1.1.0",
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_url=docker_socket,
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[secchi_prediction, kalman_filter_parameters, kalman_filter_results]
    )
    shapefile_generator = DockerOperator(
        task_id="shapefile_generator",
        image="registry.gitlab.com/cicata/water_quality_prediction/shapefile_generator:1.1.0",
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_url=docker_socket,
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[secchi_prediction, water_masks]
    )


    pixel_value_extractor >> secchi_predictor >> kalman_filter >> shapefile_generator

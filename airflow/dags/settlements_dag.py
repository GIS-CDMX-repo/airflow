from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from docker.types import Mount, DeviceRequest
import os

downloads_folder = Mount(
    source="/media/WebServerBackup2TB/data/geotiffs/sentinel",
    target="/app/downloads",
    type="bind"
)
resources_folder = Mount(source="/opt/resources", target="/app/resources", type="bind")
clippings_folder = Mount(source="/opt/clippings", target="/app/clippings", type="bind")
masks_folder = Mount(source="/opt/masks_patches", target="/app/masks_patches", type="bind")
pnas_folder = Mount(
    source="/media/WebServerBackup2TB/data/settlements/settlements-masks",
    target="/app/PNAs",
    type="bind"
)
kalman_filter_folder = Mount(
    source="/media/WebServerBackup2TB/data/settlements/csv-files",
    target="/app/kalman_filter_results",
    type="bind"
)
cloud_masks_folder = Mount(source="/opt/cloud_masks", target="/app/cloud_masks", type="bind")
pnas_uploader_folder = Mount(
    source="/media/WebServerBackup2TB/data/settlements/settlements-masks",
    target="/media/WebServerBackup2TB/data/settlements/settlements-masks",
    type="bind"
)
docker_socket="unix://var/run/docker.sock"
gitlab_connection = "gitlab_registry"
max_cloud_percentage = Variable.get(
    "SETTLEMENTS_MAX_CLOUD_PERCENTAGE",
    default_var=10
)
max_nodata_pixels = Variable.get(
    "SETTLMENTS_MAX_NODATA_PIXELS",
    default_var=10
)
cloud_mask_threshold = Variable.get(
    "SETTLEMENTS_CLOUD_THRESHOLD",
    default_var=4
)
username = Variable.get(
    "SETTLEMENTS_USERNAME"
)
password = Variable.get(
    "SETTLEMENTS_PASSWORD"
)

with DAG(
	"settlements_dag",
	start_date=datetime(2018,10,28),
	schedule_interval="0 0 28 */3 *",
	catchup=True,
    concurrency=1,
    max_active_runs=1,
    render_template_as_native_obj=True
) as dag:
    images_downloader = DockerOperator(
            task_id="images_downloader",
            image="registry.gitlab.com/cicata/satellite-imagery-utilities/downloader:2.1.0",
            docker_url=docker_socket,
            environment={
                "ATTEMPT": "{{ task_instance.try_number }}",
                "END_DATE": "{{ next_execution_date | ds  }}",
                "MAX_CLOUD_PERCENTAGE": max_cloud_percentage,
                "MAX_NODATA_PIXELS": max_nodata_pixels,
                "USERNAME": username,
                "PASSWORD": password
            },
            docker_conn_id=gitlab_connection,
            retries=5,
            retry_delay=timedelta(minutes=90),
            mount_tmp_dir=False,
            mounts=[downloads_folder],
            retrieve_output=True,
            retrieve_output_path="/app/output.json"
    )
    bands_merger = DockerOperator(
            task_id="bands_merger",
            image="registry.gitlab.com/cicata/satellite-imagery-utilities/jp2tiff:3.2.0",
            docker_url=docker_socket,
            environment={
                "END_DATE": "{{ next_execution_date | ds  }}",
                "CLOUD_THRESHOLD": cloud_mask_threshold
            },
            docker_conn_id=gitlab_connection,
            retries=5,
            retry_delay=timedelta(minutes=5),
            mount_tmp_dir=False,
            mounts=[downloads_folder, cloud_masks_folder]
    )
    tiff_clipper = DockerOperator(
        task_id="tiff_clipper",
        image="registry.gitlab.com/cicata/satellite-imagery-utilities/tiffclipper:1.3.1",
        docker_url=docker_socket,
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[downloads_folder,resources_folder,clippings_folder]
    )
    settlements_detector = DockerOperator(
        task_id="settlements_detector",
        image="registry.gitlab.com/cicata/settlements-detection:1.2.0",
        docker_url=docker_socket,
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[resources_folder],
        device_requests=[DeviceRequest(count=-1,capabilities=[["gpu"]])]
    )
    masks_merger = DockerOperator(
        task_id="masks_merger",
        image="registry.gitlab.com/cicata/satellite-imagery-utilities/tiffmerger:1.2.0",
        docker_url=docker_socket,
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[resources_folder,masks_folder]
    )
    pnas_filter = DockerOperator(
        task_id="pnas_filter",
        image="registry.gitlab.com/cicata/satellite-imagery-utilities/filter_pnas:2.1.2",
        docker_url=docker_socket,
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[resources_folder,pnas_folder, cloud_masks_folder]
    )
    kalman_filter = DockerOperator(
        task_id="kalman_filter",
        image="registry.gitlab.com/cicata/satellite-imagery-utilities/kalman_filter:1.0.1",
        docker_url=docker_socket,
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[pnas_folder, kalman_filter_folder]
    )
    shapefile_uploader = DockerOperator(
        task_id = "shapefile_uploader",
        image="registry.gitlab.com/cicata/geoserver-tools/ogr2ogr:1.1.0",
        docker_url=docker_socket,
        environment={
            "END_DATE": "{{ next_execution_date | ds  }}"
        },
        docker_conn_id=gitlab_connection,
        retries=5,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[pnas_uploader_folder],
        command="settlements",
        network_mode="gis_network"
    )


    images_downloader >> bands_merger >> tiff_clipper >> settlements_detector >> masks_merger >> pnas_filter >> kalman_filter >> shapefile_uploader

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    define_asset_job,
    EnvVar,
    load_assets_from_modules,
    ScheduleDefinition,
)
from dagster_gcp.gcs import GCSPickleIOManager, GCSResource

from rives import Rives
from . import assets, resources, cartridge_assets


all_assets = load_assets_from_modules([assets, cartridge_assets])

io_manager = {}

if EnvVar('IO_MANAGER_GCS_BUCKET').get_value() is not None:
    io_manager = {
        'io_manager': GCSPickleIOManager(
            gcs_bucket=EnvVar("IO_MANAGER_GCS_BUCKET"),
            gcs_prefix=EnvVar("IO_MANAGER_GCS_PREFIX"),
            gcs=GCSResource(project=EnvVar("IO_MANAGER_GCS_PROJECT"))
        ),
    }

leaderboard_job = define_asset_job(
    name='leaderboard_job',
    selection=AssetSelection.groups('leaderboard')
)

leaderboard_schedule = ScheduleDefinition(
    job=leaderboard_job,
    cron_schedule="*/4 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=all_assets,
    jobs=[
        leaderboard_job,
    ],
    schedules=[
        leaderboard_schedule,
    ],
    resources={
        'rives': Rives(
            inspect_url_prefix=EnvVar('INSPECT_URL_PREFIX').get_value(),
            graphql_url_prefix=EnvVar('GRAPHQL_URL_PREFIX').get_value(),
        ),
        'public_bucket': resources.PublicBucket(
            base_path=EnvVar('PUBLIC_BUCKET_BASE_PATH')
        ),
        'aggregator': resources.Aggregator(
            base_url=EnvVar('AGGREGATOR_URL_PREFIX')
        ),
        **io_manager,
    },
    sensors=[
        cartridge_assets.cartridge_sensor,
    ]
)

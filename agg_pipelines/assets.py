import datetime
from typing import Dict

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    ResourceParam,
)
import pandas as pd

from rives import Rives, Notice

from .leaderboard import (
    compute_leaderboard,
    leaderboard_frontend_data
)
from .resources import PublicBucket


@asset(group_name='leaderboard')
def tournament_contests() -> list[Dict[str, str]]:
    # TODO: Retrieve fom configuration file (in GCS as well?)
    return [
        {
            'contest_id': '2990bb8dc10d2990bb8dc10d7344e953be5362cd',
            'name': 'Lightning Run',
        },
        {
            'contest_id': '2990bb8dc10d2990bb8dc10d49311acf7d414378',
            'name': 'Knuckle Crusher',
        },
        {
            'contest_id': '2990bb8dc10d2990bb8dc10dbc0c9a3c8f9d03de',
            'name': 'Infallible Aim',
        },
        {
            'contest_id': '2990bb8dc10d2990bb8dc10d8baf804234f09e2e',
            'name': 'Secret Master',
        },
        {
            'contest_id': '2990bb8dc10d2990bb8dc10dde8c8af042857714',
            'name': 'Treasure Seeker',
        },
        {
            'contest_id': '2990bb8dc10d2990bb8dc10d96345ea884a19e7d',
            'name': 'The Completionist',
        },
        {
            'contest_id': '2990bb8dc10d2990bb8dc10d98e1aecc0ec47a04',
            'name': 'Enemy Eradicator',
        },
    ]


@asset(group_name='leaderboard')
def tournament_notices(
    context: AssetExecutionContext,
    tournament_contests: list[Dict[str, str]],
    rives: ResourceParam[Rives],
) -> list[Notice]:
    """
    A list of notices generated by the validataion of tapes.
    """
    contest_ids = [x['contest_id'] for x in tournament_contests]
    notices = rives.get_contests_scores(contest_ids)

    context.add_output_metadata(
        metadata={
            "n_contests": len(tournament_contests),
            "n_notices": len(notices),
        }
    )

    return notices


@asset(group_name='leaderboard')
def tournament_leaderboard(
    context: AssetExecutionContext,
    tournament_contests: list[Dict[str, str]],
    tournament_notices: list[Notice],
) -> pd.DataFrame:

    contest_ids = [x['contest_id'] for x in tournament_contests]

    leaderboard = compute_leaderboard(
        notices=tournament_notices,
        contest_ids=contest_ids,
    )

    context.add_output_metadata(
        metadata={
            'leaderboard': MetadataValue.md(leaderboard.to_markdown()),
        }
    )

    return leaderboard


@asset(group_name='leaderboard')
def tournament_leaderboard_file(
    context: AssetExecutionContext,
    tournament_leaderboard: pd.DataFrame,
    tournament_contests: list[Dict[str, str]],
    public_bucket: PublicBucket,
):

    data = leaderboard_frontend_data(
        df_leaderboard=tournament_leaderboard,
        contests=tournament_contests,
    )

    data['run'] = {
        'executed': (
            datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        ),
        'run_id': context.run.run_id,
    }

    output_file = public_bucket.write_tournament_leaderboard(
        data,
        'doom-olympics'
    )

    context.log.info('Wrote leaderboard to %s', output_file)

    return MaterializeResult(
        metadata={
            'dagster/uri': output_file
        }
    )

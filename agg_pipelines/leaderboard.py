"""
Leaderboard data manipulation
"""
import datetime
from typing import Dict

import pandas as pd
from pandas.io.formats.style import Styler

import numpy as np

from rives import Notice


def _dataframe_from_notices(notices: list[Notice]):
    """
    Create a DataFrame from the notices
    """
    records = [
        {
            'input_index': x.input_index,
            'output_index': x.output_index,
            'block_number': x.block_number,
            'verification_timestamp': x.block_timestamp,
            'submission_timestamp': datetime.datetime.fromtimestamp(
                x.payload.timestamp, tz=datetime.timezone.utc
            ),
            'cartridge_id': x.payload.cartridge_id.hex(),
            'rule_id': x.payload.rule_id[:20].hex(),
            'tape_id': x.payload.tape_id.hex(),
            'score': x.payload.score,
            'user_address': x.payload.user_address,
            'error_code': x.payload.error_code,
            'proof': x.proof,
        }
        for x in notices
    ]
    df = (
        pd.DataFrame.from_records(records)
        .query('error_code == 0')
    )
    return df


def _points_by_rank_linear(rank: int) -> float:
    """
    Give a number of points based on the player rank inside the contest
    """
    if rank > 1000:
        return np.exp(1000 - rank)
    return float(1001 - rank)


def _points_by_rank(rank: int):
    if rank <= 10:
        return 1000 - 3 * (rank - 1)
    if rank <= 100:
        return 973 - 2 * (rank - 10)
    return 793 - (rank - 100)


def _score_contest(group: pd.DataFrame, all_players: list[str] | None = None):
    """
    Create scores for a single contest
    """
    ranked_group = (
        group
        .sort_values(by='score', ascending=False)
        .drop_duplicates(subset=['user_address'], keep='first')
        .query('score > 0')
        .assign(rank=lambda x: range(1, x.shape[0] + 1))
    )

    if all_players is not None:
        missing_rank = ranked_group.shape[0] + 1

        players = set(ranked_group['user_address'])

        cartridge_id = ranked_group['cartridge_id'].iloc[0]

        missing_players = [x for x in all_players if x not in players]
        missing_players_df = pd.DataFrame(
            [
                {
                    'cartridge_id': cartridge_id,
                    'user_address': player,
                    'rank': missing_rank,
                }
                for player in missing_players
            ]
        )
        ranked_group = pd.concat([ranked_group, missing_players_df], axis=0)

    ranked_group = ranked_group.assign(
        points=lambda x: x['rank'].apply(_points_by_rank)
    )
    return ranked_group


def _compute_contest_scores(df_notices: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a DataFrame of notices into a narrow-form dataframe of contest
    scores.
    """
    all_players = df_notices['user_address'].unique()

    return (
        df_notices
        .groupby('rule_id')
        .apply(_score_contest, include_groups=False, all_players=all_players)
        .reset_index()
    )


def _pivot_scores(
    df_scores: pd.DataFrame,
    contest_ids: list[str]
) -> pd.DataFrame:
    """
    Transform a narrow-form dataframe of contest scores into the final pivoted
    leaderboard.
    """
    title_order = contest_ids + ['rank', 'points', 'score', 'tape_id']

    pivoted = (
        pd.pivot(
            df_scores,
            columns='rule_id',
            index='user_address',
            values=['points', 'score', 'rank', 'tape_id']
        )
        .assign(
            total_points=lambda x: x['points'].fillna(0.0).sum(axis=1),
            total_score=lambda x: x['score'].fillna(0.0).sum(axis=1)
        )
        .sort_values(by='total_points', ascending=False)
        .swaplevel(axis=1)
        .sort_index(
            axis=1,
            kind='stable',
            key=lambda x: x.map(
                lambda y: title_order.index(y) if y in title_order else -1)
            )
    )
    pivoted.index.name = None
    pivoted.columns.names = [None, None]

    return pivoted


def compute_leaderboard(
    notices: list[Notice],
    contest_ids: list[str]
) -> pd.DataFrame:
    """
    Return the pivoted leaderboard based on a list of notices
    """

    df_notices = _dataframe_from_notices(notices)
    df_scores = _compute_contest_scores(df_notices=df_notices)
    df_pivoted = _pivot_scores(df_scores=df_scores, contest_ids=contest_ids)
    return df_pivoted


def style_leaderboard(
    df_leaderboard: pd.DataFrame,
    contests: list[Dict[str, str]],
    address_book: Dict[str, str] = {},
    tape_url_prefix: str = 'https://vanguard.rives.io/tapes',
) -> Styler:

    def format_user_address(addr: str):
        if addr in address_book:
            return address_book[addr]
        return addr[:8] + '...' + addr[-6:]

    contest_names = {x['contest_id']: x['name'] for x in contests}

    def format_title(title: str):
        if title in contest_names:
            return contest_names[title]
        return title.replace('_', ' ').title()

    if tape_url_prefix.endswith('/'):
        tape_url_prefix = tape_url_prefix[:-1]

    def format_tape(tape_id: str):
        return f'<a href="{tape_url_prefix}/{tape_id}">Tape</a>'

    cols_rank = [x for x in df_leaderboard.columns if x[1] == 'rank']
    cols_tapes = [x for x in df_leaderboard.columns if x[1] == 'tape_id']

    return (
        df_leaderboard.style
        .format_index(format_title, axis=1)
        .format_index(format_user_address, axis=0)
        .format(na_rep='', precision=0)
        .format(format_tape, subset=cols_tapes, na_rep='')
        .set_table_styles({
            x: [
                {'selector': 'td', 'props': 'border-left: 1px solid white'}
            ]
            for x in cols_rank
        })
    )


def leaderboard_frontend_data(
    df_leaderboard: pd.DataFrame,
    contests: list[Dict[str, str]],
) -> dict:
    """
    Convert the dataframe to the preferred format for the frontend
    """
    leaderboard = []

    for profile_address, row in df_leaderboard.iterrows():
        row_dict = {
            'profile_address': profile_address
        }
        row_contests = {}
        for (idx, val) in row.items():
            if pd.isna(val):
                continue
            if idx[0] == '':
                row_dict[idx[1]] = val
            else:
                row_contests.setdefault(idx[0], {})[idx[1]] = val
        row_dict['contests'] = row_contests
        leaderboard.append(row_dict)

    formatted_data = {
        'contests': contests,
        'created': datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        'leaderboard': leaderboard,
    }

    return formatted_data

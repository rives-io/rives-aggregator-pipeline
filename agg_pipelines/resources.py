import datetime
import json
from urllib.parse import urljoin

from dagster import ConfigurableResource
from upath import UPath

from pydantic import PrivateAttr

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class PublicBucket(ConfigurableResource):
    base_path: str = './data'

    def write_tournament_leaderboard(
        self,
        data: dict,
        tournament_name: str
    ) -> str:
        """Write the leaderboard file and return the final path"""
        base = UPath(self.base_path)

        output_dir = base / 'tournament' / tournament_name
        output_dir.mkdir(exist_ok=True, parents=True)

        output_file = output_dir / 'leaderboard.json'

        with output_file.open('wt') as fout:
            json.dump(data, fout)

        return str(output_file)


def _build_session(max_retries=3) -> requests.Session:
    """Construct a Requests session"""
    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "PUT"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class Aggregator(ConfigurableResource):
    base_url: str
    _session: requests.Session = PrivateAttr(default_factory=_build_session)

    def award_ca(
        self,
        profile_address: str,
        ca_slug: str,
        created_at: datetime.datetime | None = None,
        tape_id: str | None = None,
        comments: str | None = None,
        points: int = 0
    ):
        """Award a Console Achievement"""

        url = urljoin(self.base_url, 'agg_rw/awarded_console_achievement')
        payload = {
            'profile_address': profile_address,
            'ca_slug': ca_slug,
            'created_at': created_at,
            'points': points,
            'comments': comments,
            'tape_id': tape_id,
        }

        resp = self._session.post(url=url, json=payload)
        resp.raise_for_status()

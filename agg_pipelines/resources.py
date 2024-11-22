import datetime
import json
import logging
from typing import Generator
from urllib.parse import urljoin

from dagster import ConfigurableResource
from upath import UPath

from pydantic import PrivateAttr

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger(__name__)


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

    def gen_items(
        self,
        url: str,
        params: dict = {},
        page_size: int = 50,
    ) -> Generator[dict, None, None]:

        offset = 0

        while True:
            request_params = params.copy()
            request_params['limit'] = page_size
            request_params['offset'] = offset

            resp = self._session.get(
                url=url,
                params=request_params
            )
            resp.raise_for_status()

            data = resp.json()

            if len(data['items']) == 0:
                break

            yield from data['items']

            offset += len(data['items'])

    def gen_console_achievements_for_profile(
        self,
        profile_address: str,
    ) -> Generator[dict, None, None]:
        url = urljoin(
            self.base_url,
            f'agg/profile/{profile_address}/console_achievements'
        )
        yield from self.gen_items(url=url)

    def send_notification(
        self,
        profile_address: str,
        title: str,
        message: str,
        url: str,
        created_at: datetime.datetime | str | None = None,
    ):
        request_url = urljoin(self.base_url, 'agg_rw/notifications')

        if created_at is not None:
            if isinstance(created_at, datetime.datetime):
                created_at = created_at.isoformat()

        payload = {
            'profile_address': profile_address,
            'title': title,
            'message': message,
            'url': url,
            'created_at': created_at,
        }

        resp = self._session.put(url=request_url, json=payload)
        resp.raise_for_status()

    def put_cartridge(
        self,
        cartridge_id: str,
        name: str | None = None,
        authors: str | None = None,
        created_at: datetime.datetime | str | None = None,
        creator_address: str | None = None,
        buy_value: int | None = None,
        sell_value: int | None = None,
    ):
        url = urljoin(self.base_url, 'agg_rw/cartridge')

        if isinstance(created_at, datetime.datetime):
            created_at = created_at.isoformat()

        payload = {
            'id': cartridge_id,
            'name': name,
            'authors': authors,
            'buy_value': buy_value,
            'sell_value': sell_value,
            'created_at': created_at,
            'creator_address': creator_address,
        }

        resp = self._session.put(url=url, json=payload)
        resp.raise_for_status()

    def put_tape(
        self,
        tape_id: str,
        name: str | None = None,
        score: int | None = None,
        title: str | None = None,
        buy_value: int | None = None,
        sell_value: int | None = None,
        created_at: datetime.datetime | str | None = None,
        creator_address: str | None = None,
        rule_id: str | None = None,
    ):
        url = urljoin(self.base_url, 'agg_rw/tape')

        if created_at is not None:
            if isinstance(created_at, datetime.datetime):
                created_at = created_at.isoformat()

        payload = {
            'id': tape_id,
            'name': name,
            'score': score,
            'title': title,
            'buy_value': buy_value,
            'sell_value': sell_value,
            'created_at': created_at,
            'creator_address': creator_address,
            'rule_id': rule_id,
        }

        resp = self._session.put(url=url, json=payload)
        resp.raise_for_status()

    def put_rule(
        self,
        rule_id: str,
        name: str | None = None,
        description: str | None = None,
        created_at: datetime.datetime | str | None = None,
        start: datetime.datetime | str | None = None,
        end: datetime.datetime | str | None = None,
        cartridge_id: str | None = None,
        created_by: str | None = None,
        sponsor_name: str | None = None,
        prize: str | None = None,
    ):
        url = urljoin(self.base_url, 'agg_rw/rule')

        if isinstance(created_at, datetime.datetime):
            created_at = created_at.isoformat()

        if isinstance(start, datetime.datetime):
            start = start.isoformat()

        if isinstance(end, datetime.datetime):
            end = end.isoformat()

        payload = {
            'id': rule_id,
            'name': name,
            'description': description,
            'cartridge_id': cartridge_id,
            'created_by': created_by,
            'sponsor_name': sponsor_name,
            'prize': prize,
            'start': start,
            'end': end,
        }

        resp = self._session.put(url=url, json=payload)
        resp.raise_for_status()


class GifServer(ConfigurableResource):
    base_url: str
    _session: requests.Session = PrivateAttr(default_factory=_build_session)

    def get_names(
        self,
        tape_ids: list[str],
    ):

        url = urljoin(self.base_url, 'names')
        resp = self._session.post(url=url, json=tape_ids)
        resp.raise_for_status()

        data = resp.json()

        return dict(zip(tape_ids, data))

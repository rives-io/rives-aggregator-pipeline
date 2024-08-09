"""
Rives Adapter
"""
import datetime
import json
from typing import Iterable

from cartesi.abi import Bytes32, Int, Address, UInt, String, decode_to_model
from pydantic import BaseModel
import requests
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

NOTICE_FIELDS_FRAGMENT = """
fragment noticeFields on Notice {
  index
  input {
    index
    timestamp
    msgSender
    blockNumber
  }
  payload
  proof {
    validity {
      inputIndexWithinEpoch
      outputIndexWithinInput
      outputHashesRootHash
      vouchersEpochRootHash
      noticesEpochRootHash
      machineStateHash
      outputHashInOutputHashesSiblings
      outputHashesInEpochSiblings
    }
    context
  }
}
"""


class VerificationOutput(BaseModel):
    version:                Bytes32
    cartridge_id:           Bytes32
    cartridge_input_index:  Int
    user_address:           Address
    timestamp:              UInt
    score:                  Int
    rule_id:                String
    rule_input_index:       Int
    tape_hash:              Bytes32
    tape_input_index:       Int
    error_code:             UInt


class NoticeProof(BaseModel):
    input_index_within_epoch: int
    output_index_within_input: int
    output_hashes_root_hash: str
    vouchers_epoch_root_hash: str
    notices_epoch_root_hash: str
    machine_state_hash: str
    output_hash_in_output_hashes_siblings: list[str]
    output_hashes_in_epoch_siblings: list[str]
    context: str

    @classmethod
    def parse_graphql_response(cls, resp: dict):
        val = resp['validity']
        params = {
            'input_index_within_epoch': val['inputIndexWithinEpoch'],
            'output_index_within_input': val['outputIndexWithinInput'],
            'output_hashes_root_hash': val['outputHashesRootHash'],
            'vouchers_epoch_root_hash': val['vouchersEpochRootHash'],
            'notices_epoch_root_hash': val['noticesEpochRootHash'],
            'machine_state_hash': val['machineStateHash'],
            'output_hash_in_output_hashes_siblings':
                val['outputHashInOutputHashesSiblings'],
            'output_hashes_in_epoch_siblings':
                val['outputHashesInEpochSiblings'],
            'context': resp['context'],
        }
        return cls.parse_obj(params)


class Notice(BaseModel):
    input_index: int
    output_index: int
    msg_sender: str
    block_number: int
    block_timestamp: datetime.datetime
    payload: VerificationOutput
    proof: NoticeProof | None = None


class OutputPointer(BaseModel):
    type: str
    module: str
    class_name: str
    input_index: int
    output_index: int


def _decode_inspect(response: dict) -> list[dict]:
    assert response.get('status') == 'Accepted'

    reports = []

    for report in response.get('reports', []):
        payload = bytes.fromhex(report['payload'][2:])

        try:
            decoded = json.loads(payload.decode('utf-8'))
        except Exception:
            decoded = {'__raw': payload}
        reports.append(decoded)
    return reports


class Rives:

    def __init__(self, url_prefix: str, validator_addr: str) -> None:
        self.url_prefix: str = url_prefix
        self.validator_addr: str = validator_addr
        self.session = requests.Session()
        self.graphql: Client = self._get_graphql_client()

    def _get_graphql_client(self) -> Client:
        transport = RequestsHTTPTransport(url=self._assemble_url('/graphql'))
        client = Client(transport=transport)
        return client

    def _assemble_url(self, suffix: str):

        prefix = self.url_prefix
        if prefix.endswith('/'):
            prefix = prefix[:-1]

        if suffix.startswith('/'):
            suffix = suffix[1:]

        return prefix + '/' + suffix

    def _inspect_scores(self, contest_id: str, n_records: int = 100):

        url = self._assemble_url('inspect/indexer/indexer_query')

        params = {
            'tags': [
                'score',
                contest_id,
            ],
            'type': 'notice',
            'order_by': 'value',
            'order_dir': 'desc',
            'page': 1,
            'page_size': n_records,
        }

        resp = self.session.get(url, params=params)

        reports = _decode_inspect(resp.json())

        assert len(reports) == 1, "Expected only one report."
        return [OutputPointer.parse_obj(x) for x in reports[0]['data']]

    def _get_graphql_query(self, pointers: list[OutputPointer]) -> str:
        assert pointers
        queries = []

        for idx, pointer in enumerate(pointers):
            alias = f'output_{idx}'

            if pointer.type != 'notice':
                continue

            queries.append(
                f'{alias}: notice(inputIndex: {pointer.input_index}, '
                f'noticeIndex: {pointer.output_index}) {{...noticeFields}}'
            )

        final_query = f'query {{\n{"\n".join(queries)}\n}}\n'
        final_query += NOTICE_FIELDS_FRAGMENT
        return final_query

    def _resolve_notices(self, pointers: list[OutputPointer]) -> list[Notice]:
        if not pointers:
            return []

        query = self._get_graphql_query(pointers)
        query = gql(query)
        resp = self.graphql.execute(query)

        notices = []

        for entry in resp.values():
            proof = NoticeProof.parse_graphql_response(entry.get('proof'))

            bytes_payload = bytes.fromhex(entry['payload'][2:])
            payload = decode_to_model(data=bytes_payload,
                                      model=VerificationOutput)  # type: ignore

            notice = Notice(
                input_index=entry['input']['index'],
                output_index=entry['index'],
                msg_sender=entry['input']['msgSender'],
                block_number=entry['input']['blockNumber'],
                block_timestamp=entry['input']['timestamp'],
                payload=payload,
                proof=proof,

            )

            notices.append(notice)

        return notices

    def get_contests_scores(
        self,
        contest_ids: Iterable[str],
        n_records: int = 100
    ) -> list[Notice]:

        pointers = []
        for contest_id in contest_ids:
            pointers.extend(self._inspect_scores(contest_id=contest_id,
                                                 n_records=n_records))

        notices = self._resolve_notices(pointers)
        return notices


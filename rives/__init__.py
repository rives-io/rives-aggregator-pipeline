"""
Rives Adapter
"""
import datetime
import json
from typing import Annotated, Iterable, List, Generic, TypeVar

from cartesi.abi import (Bytes32, Int, Address, UInt, String, decode_to_model,
                         ABIType, Bytes)
from pydantic import BaseModel
from pydantic.generics import GenericModel
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

INPUT_FIELDS_FRAGMENT = """
fragment inputFields on Input {
  index
  status
  msgSender
  timestamp
  blockNumber
  payload
}
"""


class VerificationOutputOld(BaseModel):
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


Bytes32List = Annotated[List[bytes], ABIType('bytes32[]')]


class VerificationOutput(BaseModel):
    version:                Bytes32
    cartridge_id:           Bytes32
    cartridge_input_index:  Int
    cartridge_user_address: Address
    user_address:           Address
    timestamp:              UInt
    score:                  Int
    rule_id:                Bytes32
    rule_input_index:       Int
    tape_id:                Bytes32
    tape_input_index:       Int
    error_code:             UInt
    tapes:                  Bytes32List


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


class InputPointer(BaseModel):
    type: str
    module: str
    class_name: str
    input_index: int
    dapp_address: str


ABIModel = TypeVar('ABIModel')


class Input(GenericModel, Generic[ABIModel]):
    index: int
    status: str
    msg_sender: str
    block_timestamp: datetime.datetime
    block_number: int
    prefix: str | None = None
    payload: ABIModel


class VerifyPayload(BaseModel):
    rule_id:        Bytes32
    outcard_hash:   Bytes32
    tape:           Bytes
    claimed_score:  Int
    tapes:          Bytes32List
    in_card:        Bytes


class Cartridge(BaseModel):
    id: str
    name: str
    user_address: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    primary: bool
    last_version: str


class CartridgeAuthor(BaseModel):
    name: str
    link: str


class CartridgeDetails(BaseModel):
    name: str
    summary: str | None
    description: str | None
    version: str | None
    status: str | None
    tags: list[str] | None
    authors: list[CartridgeAuthor] | None
    links: list[str] | None
    tapes: list[str] | None


class CartridgeInfo(BaseModel):
    id: str
    name: str
    user_address: str
    input_index: int | None
    authors: list[str] | None
    info: CartridgeDetails | None
    original_info: CartridgeDetails | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    cover: str | None
    active: bool | None
    primary: bool | None
    primary_id: str | None
    last_version: str | None
    versions: list[str] | None
    tapes: list[str] | None
    tags:  list[str] | None


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

    def __init__(
        self,
        inspect_url_prefix: str,
        graphql_url_prefix: str
    ) -> None:
        self.inspect_url_prefix: str = inspect_url_prefix
        self.graphql_url_prefix: str = graphql_url_prefix

        self.session = requests.Session()
        self.graphql: Client = self._get_graphql_client()

    def _get_graphql_client(self) -> Client:
        transport = RequestsHTTPTransport(url=self.graphql_url_prefix)
        client = Client(transport=transport)
        return client

    def _assemble_inspect_url(self, suffix: str):

        prefix = self.inspect_url_prefix
        if prefix.endswith('/'):
            prefix = prefix[:-1]

        if suffix.startswith('/'):
            suffix = suffix[1:]

        return prefix + '/' + suffix

    def _inspect(self, url_suffix: str, params: dict = {}):

        url = self._assemble_inspect_url(url_suffix)
        resp = self.session.get(url, params=params)
        reports = _decode_inspect(resp.json())
        return reports

    def _inspect_scores(
        self,
        contest_id: str | None = None,
        n_records: int = 100
    ):
        tags = ['score']
        if contest_id is not None:
            tags.append(contest_id)

        params = {
            'tags': tags,
            'type': 'notice',
            'order_by': 'value',
            'order_dir': 'desc',
            'page': 1,
            'page_size': n_records,
        }

        reports = self._inspect('indexer/indexer_query', params=params)

        assert len(reports) == 1, "Expected only one report."
        return [OutputPointer.parse_obj(x) for x in reports[0]['data']]

    def _inspect_tapes(self, n_records: int = 100):

        url = self._assemble_inspect_url('indexer/indexer_query')

        params = {
            'tags': [
                'tape',
            ],
            'type': 'input',
            'order_by': 'timestamp',
            'order_dir': 'desc',
            'page': 1,
            'page_size': n_records,
        }

        resp = self.session.get(url, params=params)

        reports = _decode_inspect(resp.json())

        assert len(reports) == 1, "Expected only one report."
        return [InputPointer.parse_obj(x) for x in reports[0]['data']]

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

    def _get_inputs_graphql_query(self, pointers: list[InputPointer]) -> str:
        assert pointers
        queries = []

        for idx, pointer in enumerate(pointers):
            alias = f'input_{idx}'

            if pointer.type != 'input':
                continue

            queries.append(
                f'{alias}: input(index: {pointer.input_index})'
                f' {{...inputFields}}'
            )

        final_query = f'query {{\n{"\n".join(queries)}\n}}\n'
        final_query += INPUT_FIELDS_FRAGMENT
        return final_query

    def _resolve_notices(self, pointers: list[OutputPointer]) -> list[Notice]:
        if not pointers:
            return []

        query = self._get_graphql_query(pointers)
        query = gql(query)
        resp = self.graphql.execute(query)

        notices = []

        for entry in resp.values():
            proof = None
            raw_proof = entry.get('proof')
            if raw_proof is not None:
                proof = NoticeProof.parse_graphql_response(raw_proof)

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

    def _resolve_inputs(
        self,
        pointers: list[InputPointer],
        payload_class: BaseModel,
        proxy: bool = True,
    ) -> list[Input]:
        if not pointers:
            return []

        query = self._get_inputs_graphql_query(pointers)
        query = gql(query)
        resp = self.graphql.execute(query)

        inputs = []

        for entry in resp.values():

            bytes_payload = bytes.fromhex(entry['payload'][2:])
            if proxy:
                data_payload = bytes_payload[24:]
                msg_sender = '0x' + bytes_payload[4:24].hex()
            else:
                data_payload = bytes_payload[4:]
                msg_sender = entry['msgSender']

            payload = decode_to_model(data=data_payload,
                                      model=payload_class)  # type: ignore

            input = Input[payload_class](
                index=entry['index'],
                status=entry['status'],
                msg_sender=msg_sender,
                block_number=entry['blockNumber'],
                block_timestamp=entry['timestamp'],
                payload=payload,

            )

            inputs.append(input)

        return inputs

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

    def list_cartridges(self) -> list[Cartridge]:

        reports = self._inspect('core/cartridges')

        assert len(reports) == 1, "Expected only one report."
        return [Cartridge.parse_obj(x) for x in reports[0]['data']]

    def get_cartridge_info(self, cartridge_id: str):

        reports = self._inspect(
            'core/cartridge_info',
            params={
                'id': cartridge_id
            }
        )

        assert len(reports) == 1, "Expected only one report."
        return CartridgeInfo.parse_obj(reports[0])

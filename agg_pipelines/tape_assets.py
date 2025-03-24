from base64 import b64encode
from math import tan
import datetime
import json
from typing import List
from hashlib import sha256
import base64

import dagster as dg

from .resources import Aggregator, GifServer
from rives import Input, InputPointer, Rives, Notice, OutputPointer, VerifyPayload, truncate_rule_id_from_bytes, truncate_tape_id_from_bytes
from socket import fromfd


class TapeRunConfig(dg.Config):
    output_pointer: dict


tape_parts = dg.DynamicPartitionsDefinition(name='tape')


@dg.asset(
    group_name='tape',
    partitions_def=tape_parts,
)
def tape_notice(
    config: TapeRunConfig,
    rives: dg.ResourceParam[Rives],
) -> Notice:

    ptr = OutputPointer.parse_obj(config.output_pointer)
    assert ptr.class_name == 'VerificationOutput'

    notices = rives._resolve_notices(pointers=[ptr])
    assert len(notices) == 1, "Failed to retrieve notice"

    return notices[0]


@dg.sensor(target=[tape_notice], minimum_interval_seconds=60)
def tape_sensor(
    context: dg.SensorEvaluationContext,
    rives: dg.ResourceParam[Rives],
) -> dg.SensorResult:

    cursor = int(context.cursor or 0)
    page_size = 500
    page = cursor//page_size + 1
    ptrs = rives._inspect_scores(n_records=page_size,page=page)

    part_keys = [f'{x.type}/{x.input_index}/{x.output_index}' for x in ptrs]

    new_keys = [
        x for x in part_keys
        if not tape_parts.has_partition_key(
            x,
            dynamic_partitions_store=context.instance
        )
    ]

    runs = []
    for ptr in ptrs:
        ptr_dict = json.loads(ptr.json())
        run_config = dg.RunConfig(
            ops={
                'tape_notice': TapeRunConfig(output_pointer=ptr_dict),
            }
        )
        ptr_key = f'{ptr.type}/{ptr.input_index}/{ptr.output_index}'
        runs.append(
            dg.RunRequest(
                run_key=ptr_key,
                partition_key=ptr_key,
                run_config=run_config
            )
        )

    part_requests = tape_parts.build_add_request(partition_keys=new_keys)

    context.update_cursor(str(cursor + len(new_keys)))
    return dg.SensorResult(
        run_requests=runs,
        dynamic_partitions_requests=[part_requests]
    )


@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=tape_parts,
)
def tape_record(
    aggregator: Aggregator,
    gif_server: GifServer,
    tape_notice: Notice,
):
    tape_id = tape_notice.payload.tape_id.hex()

    names = gif_server.get_names([tape_id])

    created_at = datetime.datetime.fromtimestamp(
        tape_notice.payload.timestamp,
        tz=datetime.UTC
    ).isoformat()

    aggregator.put_tape(
        tape_id=tape_id,
        name=names[tape_id],
        score=tape_notice.payload.score,
        rule_id=tape_notice.payload.rule_id[:20].hex(),
        created_at=created_at,
        creator_address=tape_notice.payload.user_address,

    )


@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=tape_parts,
)
def tape_creator_achievement(
    context: dg.AssetExecutionContext,
    aggregator: Aggregator,
    tape_notice: Notice,
):

    if tape_notice.payload.error_code != 0:
        context.log.info(
            f'Error code is {tape_notice.payload.error_code}. Skipping.'
        )
        return

    profile_address = tape_notice.payload.user_address
    tape_id = tape_notice.payload.tape_id.hex()
    tape_timestamp = datetime.datetime.fromtimestamp(
        tape_notice.payload.timestamp,
        tz=datetime.UTC
    ).isoformat()

    existing_achievement = aggregator.gen_console_achievements_for_profile(
        profile_address=profile_address
    )

    existing_achievement = [
        x for x in existing_achievement
        if x['ca_slug'] == 'tape-creator' and x['tape_id'] == tape_id
    ]

    if len(existing_achievement) == 0:
        # Send a notification if this is the first time awarding this
        # achievement
        aggregator.send_notification(
            profile_address=profile_address,
            title='New Achievement',
            message='Tape Creator',
            url=f'/tapes/{tape_id}',
            created_at=tape_timestamp,
        )

    aggregator.award_ca(
        profile_address=profile_address,
        ca_slug='tape-creator',
        created_at=tape_timestamp,
        comments=f'Created tape {tape_id} with score '
                 f'{tape_notice.payload.score}'
    )

@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=tape_parts,
)
def tape_record_data(
    aggregator: Aggregator,
    tape_notice: Notice,
    rives: dg.ResourceParam[Rives],
):
    tape_id = truncate_tape_id_from_bytes(tape_notice.payload.tape_id).hex()

    pointer = InputPointer(
        type='input',
        input_index=tape_notice.payload.tape_input_index,
        module='core',
        class_name='VerifyPayload',
        dapp_address='',
    )

    tape_inputs: List[Input[VerifyPayload]] = rives._resolve_inputs([pointer],VerifyPayload)

    assert len(tape_inputs) == 1, "Failed to retrieve tape inputs"

    tape_input = tape_inputs[0]

    rule_id_bytes = truncate_rule_id_from_bytes(tape_notice.payload.rule_id)

    incard = rives.format_incard(rule_id_bytes.hex(), tape_input.payload.in_card.hex(), [truncate_tape_id_from_bytes(x).hex() for x in tape_input.payload.tapes])
    rule = rives.get_rule(rule_id_bytes.hex())
    entropy = sha256(bytes.fromhex(tape_notice.payload.user_address[2:]) + rule_id_bytes).hexdigest()

    aggregator.put_tape(
        tape_id=tape_id,
        tape=base64.b64encode(tape_input.payload.tape).decode('utf-8'),
        incard=base64.b64encode(incard).decode('utf-8'),
        args=rule.args,
        entropy=entropy,
    )

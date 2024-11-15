import datetime
from enum import Enum
import json

import dagster as dg

from .leaderboard import _dataframe_from_notices, _score_contest
from .resources import Aggregator
from rives import Rives, Rule


class RuleRunConfig(dg.Config):
    rule: dict


rule_parts = dg.DynamicPartitionsDefinition(name='rule')


@dg.asset(
    group_name='rule',
    partitions_def=rule_parts,
)
def rule(
    config: RuleRunConfig,
) -> Rule:

    return Rule.parse_obj(config.rule)


class RuleState(str, Enum):
    continuous = 'continuous'
    scheduled = 'scheduled'
    open = 'open'
    finished = 'finished'


def get_rule_state(rule: Rule) -> RuleState:
    if rule.end is None:
        return RuleState.continuous

    assert rule.start is not None

    now = datetime.datetime.now(tz=datetime.UTC)

    if now < rule.start:
        return RuleState.scheduled

    if now < (rule.end + datetime.timedelta(minutes=5)):
        return RuleState.open

    return RuleState.finished


@dg.sensor(target=[rule], minimum_interval_seconds=60)
def rule_sensor(
    context: dg.SensorEvaluationContext,
    rives: dg.ResourceParam[Rives],
) -> dg.SensorResult:

    rules = rives.get_rules()

    part_keys = [x.id for x in rules]

    new_keys = [
        x for x in part_keys
        if not rule_parts.has_partition_key(
            x,
            dynamic_partitions_store=context.instance
        )
    ]

    runs = []
    for rule in rules:

        state = get_rule_state(rule)

        rule_dict = json.loads(rule.json())
        run_config = dg.RunConfig(
            ops={
                'rule': RuleRunConfig(rule=rule_dict),
            }
        )

        run_key = f'{rule.id}/{state}'
        runs.append(
            dg.RunRequest(
                run_key=run_key,
                partition_key=rule.id,
                run_config=run_config
            )
        )

    part_requests = rule_parts.build_add_request(partition_keys=new_keys)
    return dg.SensorResult(
        run_requests=runs,
        dynamic_partitions_requests=[part_requests]
    )


@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=rule_parts,
)
def rule_record(
    aggregator: Aggregator,
    rule: Rule,
):
    aggregator.put_rule(
        rule_id=rule.id,
        name=rule.name,
        description=rule.description,
        created_at=rule.created_at,
        created_by=rule.created_by,
        start=rule.start,
        end=rule.end,
        cartridge_id=rule.cartridge_id,
    )


@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=rule_parts,
)
def contest_winner_achievements(
    context: dg.AssetExecutionContext,
    rule: Rule,
    aggregator: Aggregator,
    rives: dg.ResourceParam[Rives],
):

    state = get_rule_state(rule=rule)
    if state != RuleState.finished:
        context.log.info(f'Rule {rule.id} is in state "{state}", not '
                         '"finished". Nothing to do.')
        return

    notices = rives.get_contests_scores(contest_ids=[rule.id], n_records=2000)
    notices = _dataframe_from_notices(notices)
    scores = _score_contest(notices)

    if scores.shape[0] >= 1:
        row = scores.iloc[0]
        aggregator.award_ca(
            profile_address=row['user_address'],
            ca_slug='contest-gold',
            created_at=row['submission_timestamp'].to_pydatetime().isoformat(),
            comments=f'You got 1st place in the "{rule.name}" contest, with a '
                     f'score of {row["score"]}',
            tape_id=row['tape_id'],
        )

        aggregator.send_notification(
            profile_address=row['user_address'],
            title='New Achievement',
            message='Contest Gold',
            url=f'/tapes/{row["tape_id"]}',
            created_at=datetime.datetime.now(tz=datetime.UTC).isoformat(),
        )

    if scores.shape[0] >= 2:
        row = scores.iloc[1]
        aggregator.award_ca(
            profile_address=row['user_address'],
            ca_slug='contest-silver',
            created_at=row['submission_timestamp'].to_pydatetime().isoformat(),
            comments=f'You got 2nd place in the "{rule.name}" contest, with a '
                     f'score of {row["score"]}',
            tape_id=row['tape_id'],
        )

        aggregator.send_notification(
            profile_address=row['user_address'],
            title='New Achievement',
            message='Contest Silver',
            url=f'/tapes/{row["tape_id"]}',
            created_at=datetime.datetime.now(tz=datetime.UTC).isoformat(),
        )

    if scores.shape[0] >= 3:
        row = scores.iloc[2]
        aggregator.award_ca(
            profile_address=row['user_address'],
            ca_slug='contest-bronze',
            created_at=row['submission_timestamp'].to_pydatetime().isoformat(),
            comments=f'You got 3rd place in the "{rule.name}" contest, with a '
                     f'score of {row["score"]}',
            tape_id=row['tape_id'],
        )

        aggregator.send_notification(
            profile_address=row['user_address'],
            title='New Achievement',
            message='Contest Bronze',
            url=f'/tapes/{row["tape_id"]}',
            created_at=datetime.datetime.now(tz=datetime.UTC).isoformat(),
        )

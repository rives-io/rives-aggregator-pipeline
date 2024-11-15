import json

import dagster as dg

from .resources import Aggregator
from rives import Rives, Cartridge


class CartridgeRunConfig(dg.Config):
    cartridge: dict


cartridge_parts = dg.DynamicPartitionsDefinition(name='cartridge')


@dg.asset(
    group_name='cartridge',
    partitions_def=cartridge_parts,
)
def cartridge(
    config: CartridgeRunConfig,
) -> Cartridge:
    # Everything comes from the sensor, we don't have to gather anything else

    return Cartridge.parse_obj(config.cartridge)


@dg.sensor(target=[cartridge], minimum_interval_seconds=120)
def cartridge_sensor(
    context: dg.SensorEvaluationContext,
    rives: dg.ResourceParam[Rives],
) -> dg.SensorResult:

    cartridges = rives.list_cartridges()
    new_ids = [
        x.id for x in cartridges
        if not cartridge_parts.has_partition_key(
            x.id,
            dynamic_partitions_store=context.instance
        )
    ]

    runs = []
    for cartridge in cartridges:
        cart_dict = json.loads(cartridge.json())
        run_config = dg.RunConfig(
            ops={
                'cartridge': CartridgeRunConfig(cartridge=cart_dict),
            }
        )
        runs.append(
            dg.RunRequest(
                run_key=f'{cartridge.id}_{cartridge.created_at.isoformat()}',
                partition_key=cartridge.id,
                run_config=run_config
            )
        )

    part_requests = cartridge_parts.build_add_request(partition_keys=new_ids)
    return dg.SensorResult(
        run_requests=runs,
        dynamic_partitions_requests=[part_requests]
    )


@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=cartridge_parts,
)
def cartridge_creator_achievement(
    context: dg.AssetExecutionContext,
    aggregator: Aggregator,
    cartridge: Cartridge,
):
    context.log.info(f'Will award achievement to user {cartridge.user_address}'
                     f' for cartridge {cartridge.id} ({cartridge.name})')

    existing_achievement = aggregator.gen_console_achievements_for_profile(
        profile_address=cartridge.user_address
    )

    existing_achievement = [
        x for x in existing_achievement
        if x['ca_slug'] == 'cartridge-creator'
    ]

    if len(existing_achievement) == 0:
        # Send a notification if this is the first time awarding this kind of
        # achievement
        aggregator.send_notification(
            profile_address=cartridge.user_address,
            title='New Achievement',
            message='Cartridge Creator',
            url=f'/cartridges/{cartridge.id}',
            created_at=cartridge.created_at,
        )

    aggregator.award_ca(
        profile_address=cartridge.user_address,
        ca_slug='cartridge-creator',
        created_at=cartridge.created_at.isoformat(),
        comments=f'Created cartridge {cartridge.name}'
    )


@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=cartridge_parts,
)
def cartridge_record(
    cartridge: Cartridge,
    aggregator: Aggregator,
    rives: dg.ResourceParam[Rives],
):
    info = rives.get_cartridge_info(cartridge.id)

    aggregator.put_cartridge(
        cartridge_id=cartridge.id,
        name=info.name,
        authors=', '.join(info.authors),
        created_at=info.created_at,
        creator_address=info.user_address,
    )

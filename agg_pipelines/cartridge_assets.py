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


@dg.sensor(target=[cartridge])
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

    aggregator.award_ca(
        profile_address=cartridge.user_address,
        ca_slug='cartridge-creator',
        created_at=cartridge.created_at.isoformat(),
        comments=f'Created cartridge {cartridge.name}'
    )

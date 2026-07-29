from dagster import Definitions

from OpenStudioLandscapes.OpenRV_Builder.definitions import assets_base
from OpenStudioLandscapes.OpenRV_Builder.definitions import sensors_base
from OpenStudioLandscapes.OpenRV_Builder.definitions import resources_base
from OpenStudioLandscapes.OpenRV_Builder.definitions import jobs_base


defs = Definitions(
    assets=[
        *assets_base,
    ],
    sensors=[
        *sensors_base
    ],
    resources={
        **resources_base,
    },
    jobs=[
        *jobs_base,
    ],
)

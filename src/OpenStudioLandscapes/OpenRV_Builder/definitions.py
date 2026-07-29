import json
import os
from typing import Dict

from OpenStudioLandscapes.engine.config.models import DockerConfigResource
from dagster import (
    Definitions,
    load_assets_from_modules,
    DefaultSensorStatus,
)

import OpenStudioLandscapes.OpenRV_Builder.assets
import OpenStudioLandscapes.OpenRV_Builder.sensors as sensors
from OpenStudioLandscapes.OpenRV_Builder.config.enums import (
    CY,
    OS,
)
from OpenStudioLandscapes.OpenRV_Builder.jobs import (
    materialize_openrv_builder_job,
    materialize_new_commit_job,
)

from OpenStudioLandscapes.OpenRV_Builder.resources import (
    NtfyResource,
    ApptainerResource,
    AutoBuilderResource,
    OpenRVCodecsResource,
    OpenRVBuilderResource,
    VFXReferencePlatformResource,
    config_NtfyResource_yaml,
    config_AutoBuilderResource_yaml,
    config_DockerConfigResource_yaml,
    config_ApptainerResource_yaml,
    config_OpenRVCodecsResource_yaml,
    config_VFXReferencePlatformResource_yaml,
    config_OpenRVBuilderResource_yaml,
)

from OpenStudioLandscapes.engine.discovery.discovery import (
    dump_yaml,
    load_yaml,
)

from OpenStudioLandscapes.OpenRV_Builder import (
    LOGGER,
    dist,
)

LOGGER.info(f"Loading {dist.name} assets...")

assets_base = load_assets_from_modules(
    modules=[OpenStudioLandscapes.OpenRV_Builder.assets],
)

### SENSORS

sensors_base = []

# Instead of setting the default Sensor status based on the environment in
# src/OpenStudioLandscapes/OpenRV_Builder/sensors.py, we can assign it
# its default value over there and override arbitrary values
# here based on the environment (`.env`).

## check_for_new_commits Sensor
env_ = os.environ.get(
    "OPENSTUDIOLANDSCPES_OPENRV_BUILDER__CHECK_FOR_NEW_COMMITS_SENSOR_STATUS",
    default=False
)
if env_:
    options_ = [i.value for i in DefaultSensorStatus]
    try:
        assert env_ in options_, f"Value for `OPENSTUDIOLANDSCPES_OPENRV_BUILDER__CHECK_FOR_NEW_COMMITS_SENSOR_STATUS` is not supported: {env_}"
    except AssertionError as e:
        LOGGER.critical(
            f"Setting `OPENSTUDIOLANDSCPES_OPENRV_BUILDER__CHECK_FOR_NEW_COMMITS_SENSOR_STATUS` "
            f"to `STOPPED`: {e}. Available options: {options_}")
        env_ = "STOPPED"
    sensors.check_for_new_commits._default_status = DefaultSensorStatus(env_)
del env_

env_ = os.environ.get(
    "OPENSTUDIOLANDSCPES_OPENRV_BUILDER__CHECK_FOR_NEW_COMMITS_SENSOR_INTERVAL",
    default=False
)
if env_:
    try:
        env_ = int(env_)
    except ValueError as e:
        default = 24
        LOGGER.critical(
            f"Can't convert `OPENSTUDIOLANDSCPES_OPENRV_BUILDER__CHECK_FOR_NEW_COMMITS_SENSOR_INTERVAL` "
            f"value to `int`. Setting to {default}.")
        env_ = 24
    sensors.check_for_new_commits._min_interval = int(3600 * env_)
del env_

sensors_base.extend(
    [
        sensors.check_for_new_commits,
    ]
)

## ntfy_on_run_success, ntfy_on_run_failure Sensors
env_ = os.environ.get(
    "OPENSTUDIOLANDSCPES_OPENRV_BUILDER__NTFY_SENSOR_STATUS",
    default=False
)
if env_:
    options_ = [i.value for i in DefaultSensorStatus]
    try:
        assert env_ in options_, f"Value for `OPENSTUDIOLANDSCPES_OPENRV_BUILDER__NTFY_SENSOR_STATUS` is not supported: {env_}"
    except AssertionError as e:
        LOGGER.critical(
            f"Setting `OPENSTUDIOLANDSCPES_OPENRV_BUILDER__NTFY_SENSOR_STATUS` "
            f"to `STOPPED`: {e}. Available options: {options_}")
        env_ = "STOPPED"
    sensors.ntfy_on_run_success._default_status = DefaultSensorStatus(env_)
    sensors.ntfy_on_run_failure._default_status = DefaultSensorStatus(env_)
del env_

sensors_base.extend(
    [
        sensors.ntfy_on_run_success,
        sensors.ntfy_on_run_failure,
    ]
)

################

config_ApptainerResource_yaml.parent.mkdir(parents=True, exist_ok=True)
if not config_ApptainerResource_yaml.exists():
    dump_yaml(
        model_config=ApptainerResource(),
        file_path=config_ApptainerResource_yaml,
    )
_yaml: Dict = load_yaml(file_path=config_ApptainerResource_yaml)
json_str = json.dumps(_yaml, indent=2)
apptainer_resource: ApptainerResource = ApptainerResource.model_validate_json(json_str)


config_NtfyResource_yaml.parent.mkdir(parents=True, exist_ok=True)
if not config_NtfyResource_yaml.exists():
    dump_yaml(
        model_config=NtfyResource(),
        file_path=config_NtfyResource_yaml,
    )
_yaml: Dict = load_yaml(file_path=config_NtfyResource_yaml)
json_str = json.dumps(_yaml, indent=2)
ntfy_resource: NtfyResource = NtfyResource.model_validate_json(json_str)


config_AutoBuilderResource_yaml.parent.mkdir(parents=True, exist_ok=True)
if not config_AutoBuilderResource_yaml.exists():
    dump_yaml(
        model_config=AutoBuilderResource(),
        file_path=config_AutoBuilderResource_yaml,
    )
_yaml: Dict = load_yaml(file_path=config_AutoBuilderResource_yaml)
json_str = json.dumps(_yaml, indent=2)
auto_builder_resource: AutoBuilderResource = AutoBuilderResource.model_validate_json(json_str)


config_DockerConfigResource_yaml.parent.mkdir(parents=True, exist_ok=True)
if not config_DockerConfigResource_yaml.exists():
    dump_yaml(
        model_config=DockerConfigResource(),
        file_path=config_DockerConfigResource_yaml,
    )
_yaml: Dict = load_yaml(file_path=config_DockerConfigResource_yaml)
json_str = json.dumps(_yaml, indent=2)
docker_config_resource: DockerConfigResource = DockerConfigResource.model_validate_json(json_str)


config_VFXReferencePlatformResource_yaml.parent.mkdir(parents=True, exist_ok=True)
if not config_VFXReferencePlatformResource_yaml.exists():
    dump_yaml(
        model_config=VFXReferencePlatformResource(
            cy=CY.CY2024,
            os=OS.LINUX,
        ),
        file_path=config_VFXReferencePlatformResource_yaml,
    )
_yaml: Dict = load_yaml(file_path=config_VFXReferencePlatformResource_yaml)
json_str = json.dumps(_yaml, indent=2)
vfx_reference_platform_resource: VFXReferencePlatformResource = VFXReferencePlatformResource.model_validate_json(json_str)


config_OpenRVCodecsResource_yaml.parent.mkdir(parents=True, exist_ok=True)
if not config_OpenRVCodecsResource_yaml.exists():
    dump_yaml(
        model_config=OpenRVCodecsResource(),
        file_path=config_OpenRVCodecsResource_yaml,
    )
_yaml: Dict = load_yaml(file_path=config_OpenRVCodecsResource_yaml)
json_str = json.dumps(_yaml, indent=2)
openrv_codecs_resource: OpenRVCodecsResource = OpenRVCodecsResource.model_validate_json(json_str)


config_OpenRVBuilderResource_yaml.parent.mkdir(parents=True, exist_ok=True)
if not config_OpenRVBuilderResource_yaml.exists():
    dump_yaml(
        model_config=OpenRVBuilderResource(),
        file_path=config_OpenRVBuilderResource_yaml,
    )
_yaml: Dict = load_yaml(file_path=config_OpenRVBuilderResource_yaml)
json_str = json.dumps(_yaml, indent=2)
openrvbuilder_resource: OpenRVBuilderResource = OpenRVBuilderResource.model_validate_json(json_str)

###

resources_base = {
    "apptainer_resource": apptainer_resource,
    "ntfy_resource": ntfy_resource,
    "auto_builder_resource": auto_builder_resource,
    "docker_config_resource": docker_config_resource,
    "vfx_reference_platform_resource": vfx_reference_platform_resource,
    "openrv_codecs_resource": openrv_codecs_resource,
    "openrvbuilder_resource": openrvbuilder_resource,
}

# Jobs
jobs_base = [
    materialize_openrv_builder_job,
    materialize_new_commit_job,
]


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

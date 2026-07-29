from dagster import (
    define_asset_job,
    AssetSelection,
)

materialize_openrv_builder_job = define_asset_job(
    name="openrv_builder_job",
    description="Materialize all OpenStudioLandscapes-OpenRV-Builder assets.",
    selection=AssetSelection.all(
        include_sources=False,
    ),
)

materialize_new_commit_job = define_asset_job(
    name="new_commit_job",
    description="",
    selection=AssetSelection.all(
        include_sources=False,
    ),
)

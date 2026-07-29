# pylint: disable=line-too-long,invalid-name
import enum
import datetime
import json
import subprocess
import urllib.parse
import shutil
import pathlib
import textwrap
import shlex
from typing import (
    Any,
    Dict,
    Generator,
    List,
)

import yaml
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetMaterialization,
    MetadataValue,
    Output,
    asset,
)
from OpenStudioLandscapes.DagsterCodeLocation.StreamingProcess import submit_cmds
from OpenStudioLandscapes.engine.config.models import DockerConfigResource
from OpenStudioLandscapes.engine.enums import ComposeCmdExclusion
from OpenStudioLandscapes.engine.utils.docker import (
    docker_build_cmd,
    docker_do,
    docker_push_cmd,
)
from OpenStudioLandscapes.engine.policies.retry import build_docker_image_retry_policy

from OpenStudioLandscapes.OpenRV_Builder import (
    ASSET_HEADER,
    dist,
)
from OpenStudioLandscapes.OpenRV_Builder.config.enums import (
    Targets,
    RVBuildTarget,
    RVToolChain,
)
from OpenStudioLandscapes.engine.utils import (
    get_docker_run_cmd,
    get_image_name,
    parse_docker_image_path,
    get_image_metadata,
)

from OpenStudioLandscapes.OpenRV_Builder.resources import (
    CloneRepoAssetConfig,
    ApptainerResource,
    OpenRVCodecsResource,
    QtPackagesConfig,
    AutoBuilderResource,
    OpenRVBuilderResource,
    VFXReferencePlatformResource,
)

ASSET_HEADER_BASE_OS = {
    **ASSET_HEADER,
    "group_name": f"{ASSET_HEADER['group_name']}_BUILD_SYSTEM_BASE"
}

ASSET_HEADER_BUILD = {
    **ASSET_HEADER,
    "group_name": f"{ASSET_HEADER['group_name']}_BUILD"
}

ASSET_HEADER_APPTAINER = {
    **ASSET_HEADER,
    "group_name": f"{ASSET_HEADER['group_name']}_APPTAINER"
}

ASSET_HEADER_TARBALL = {
    **ASSET_HEADER,
    "group_name": f"{ASSET_HEADER['group_name']}_TARBALL"
}

ASSET_HEADER_CLEANUP = {
    **ASSET_HEADER,
    "group_name": f"{ASSET_HEADER['group_name']}_CLEANUP"
}

# https://github.com/yaml/pyyaml/issues/722#issuecomment-1969292770
yaml.SafeDumper.add_multi_representer(
    data_type=enum.Enum,
    representer=yaml.representer.SafeRepresenter.represent_str,
)


exclude_from_quote = []
exclude_from_quote.extend(
    ComposeCmdExclusion.CMD_APPEND_ALWAYS_EXCLUDE_FROM_QUOTATION.value
)
exclude_from_quote.extend(
    [
        "|",
        "--mode='og-w'",
        ">",
    ]
)


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={},
)
def openrv_base_os_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM docker.io/rockylinux/rockylinux:9 AS {stage}

        LABEL org.opencontainers.image.authors="michimussato@etik.com"
        LABEL org.opencontainers.image.source="https://github.com/michimussato/OpenStudioLandscapes-OpenRV-Builder"
        LABEL org.opencontainers.image.vendor="OpenStudioLandscapes"

        SHELL ["/bin/bash", "-c"]

        ARG RV_REPO
        ENV RV_REPO=${{RV_REPO}}

        USER root
        WORKDIR /root

        # static versions:

        # Install tools and build dependencies
        RUN dnf upgrade -y \\
            && dnf install -y epel-release \\
            && dnf config-manager --set-enabled crb devel \\
            && dnf install -y perl-CPAN

        ENV PERL_MM_USE_DEFAULT=1
        RUN cpan FindBin

        RUN dnf groupinstall -y "Development Tools" \\
            && dnf install -y \\
            alsa-lib-devel \\
            autoconf \\
            automake \\
            avahi-compat-libdns_sd-devel \\
            bison \\
            bzip2-devel \\
            cmake-gui \\
            curl-devel \\
            flex \\
            gcc \\
            gcc-c++ \\
            git \\
            libX11-devel \\
            libXcomposite \\
            libXcomposite-devel \\
            libXcursor \\
            libXcursor-devel \\
            libXdamage \\
            libXext-devel \\
            libXi-devel \\
            libXrandr \\
            libXrandr-devel \\
            libXrender-devel \\
            libXtst \\
            xcb-util-cursor \\
            libXxf86vm-devel \\
            libaio-devel \\
            libffi-devel \\
            libtool \\
            libxkbcommon \\
            libxkbcommon-devel \\
            libxkbfile \\
            mesa-libGLU \\
            mesa-libGLU-devel \\
            mesa-compat-libOSMesa \\
            mesa-compat-libOSMesa-devel \\
            meson \\
            nasm \\
            ncurses-devel \\
            nss \\
            ocl-icd \\
            ocl-icd-devel \\
            opencl-headers \\
            openssl-devel \\
            patch \\
            patchelf \\
            pcsc-lite \\
            perl-FindBin \\
            perl-IPC-Cmd \\
            pulseaudio-libs \\
            pulseaudio-libs-glib2 \\
            qt5-qtbase-devel \\
            readline-devel \\
            sqlite-devel \\
            systemd-devel \\
            tcl-devel \\
            tcsh \\
            tk-devel \\
            wget \\
            xz-devel \\
            yasm \\
            zip \\
            zlib-devel \\
            && dnf clean all

        # Disable the devel repo afterwards since dnf will warn about it
        RUN dnf config-manager --set-disabled devel

        CMD ["/bin/bash"]
        """).format(
        stage=Targets.openrv_base_os,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_os_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_os_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def openrv_base_os_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    openrv_base_os_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "RV_REPO": openrvbuilder_resource.RV_REPO,
        "CMAKE_GENERATOR": openrvbuilder_resource.CMAKE_GENERATOR.value,
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'CMAKE_VERSION': '3.31.6', 'CMAKE_BASE': '/opt/cmake', 'RUSTUP_HOME': '/opt/rust', 'CARGO_HOME': '/opt/rust', 'NINJA_STATUS': '- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ', 'NINJA_HOME': '/opt/ninja', 'NINJA_VERSION': '1.12.1', 'PYENV_ROOT': '/opt/pyenv', 'PYTHON_VERSION': '3.11.15', 'QT_ROOT': '/opt/Qt', 'QT_VERSION': '6.5.3', 'QT_GCC': 'gcc_64', 'QT_MODULES': 'debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES': 'icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=openrv_base_os_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.openrv_base_os,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"openrv_base_os_build_stage cmds: {cmds}")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_os_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_os_build_stage"])
        ),
    },
)
def openrv_base_rust_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrv_base_os_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_os_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}

        LABEL org.opencontainers.image.authors="michimussato@etik.com"
        LABEL org.opencontainers.image.source="https://github.com/michimussato/OpenStudioLandscapes-OpenRV-Builder"
        LABEL org.opencontainers.image.vendor="OpenStudioLandscapes"

        SHELL ["/bin/bash", "-c"]

        ARG RV_REPO
        ENV RV_REPO=${{RV_REPO}}

        USER root
        WORKDIR /root

        # static versions:

        # Install Rust (>=1.92)
        # RUSTUP_HOME and CARGO_HOME need to persist
        # - this will reference /opt/rust/settings.toml instead of ~/.rustup/settings.toml
        # - https://aswf-openrv.readthedocs.io/en/latest/build_system/config_macos.html#install-tools-and-build-dependencies
        ARG RUSTUP_HOME
        ENV RUSTUP_HOME=${{RUSTUP_HOME}}
        RUN mkdir -p ${{RUSTUP_HOME}}
        ARG CARGO_HOME
        ENV CARGO_HOME=${{CARGO_HOME}}
        RUN mkdir -p ${{CARGO_HOME}}
        ENV PATH="${{RUSTUP_HOME}}/bin:${{CARGO_HOME}}/bin:${{PATH}}"
        RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path

        CMD ["/bin/bash"]
        """).format(
        stage=Targets.openrv_base_rust,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_rust_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_rust_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def openrv_base_rust_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    openrv_base_rust_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "RV_REPO": openrvbuilder_resource.RV_REPO,
        # Rust args
        **{
            "RUSTUP_HOME": openrvbuilder_resource.RUSTUP_HOME,
            "CARGO_HOME": openrvbuilder_resource.CARGO_HOME,
        },
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'CMAKE_VERSION': '3.31.6', 'CMAKE_BASE': '/opt/cmake', 'RUSTUP_HOME': '/opt/rust', 'CARGO_HOME': '/opt/rust', 'NINJA_STATUS': '- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ', 'NINJA_HOME': '/opt/ninja', 'NINJA_VERSION': '1.12.1', 'PYENV_ROOT': '/opt/pyenv', 'PYTHON_VERSION': '3.11.15', 'QT_ROOT': '/opt/Qt', 'QT_VERSION': '6.5.3', 'QT_GCC': 'gcc_64', 'QT_MODULES': 'debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES': 'icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=openrv_base_rust_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.openrv_base_rust,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"openrv_base_os_build_stage cmds: {cmds}")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_os_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_os_build_stage"])
        ),
    },
)
def openrv_base_cmake_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrv_base_os_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_os_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}

        LABEL org.opencontainers.image.authors="michimussato@etik.com"
        LABEL org.opencontainers.image.source="https://github.com/michimussato/OpenStudioLandscapes-OpenRV-Builder"
        LABEL org.opencontainers.image.vendor="OpenStudioLandscapes"

        SHELL ["/bin/bash", "-c"]

        ARG RV_REPO
        ENV RV_REPO=${{RV_REPO}}

        USER root
        WORKDIR /root

        # static versions:

        # Install CMake
        # - https://cmake.org/cmake/help/latest/manual/cmake-variables.7.html
        # - https://cmake.org/cmake/help/latest/manual/cmake.1.html#options
        ARG CMAKE_VERSION
        ARG CMAKE_BASE
        RUN mkdir -p ${{CMAKE_BASE}}
        ENV PATH="${{CMAKE_BASE}}/bin:${{PATH}}"
        RUN curl \\
                -L https://github.com/Kitware/CMake/releases/download/v${{CMAKE_VERSION}}/cmake-${{CMAKE_VERSION}}.tar.gz \\
                -o cmake-${{CMAKE_VERSION}}.tar.gz \\
            && tar -xzvf cmake-${{CMAKE_VERSION}}.tar.gz \\
            && pushd cmake-${{CMAKE_VERSION}} \\
            && ./bootstrap --parallel=$(nproc --all) --prefix=${{CMAKE_BASE}} \\
            && make -j $(nproc --all) \\
            && make install \\
            && popd \\
            && rm -rf cmake-${{CMAKE_VERSION}}*
        
        CMD ["/bin/bash"]
        """).format(
        stage=Targets.openrv_base_cmake,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_cmake_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_cmake_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def openrv_base_cmake_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    openrv_base_cmake_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "RV_REPO": openrvbuilder_resource.RV_REPO,
        # Cmake args
        **{
            "CMAKE_VERSION": openrvbuilder_resource.CMAKE_VERSION,
            "CMAKE_BASE": openrvbuilder_resource.CMAKE_BASE,
        },
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'CMAKE_VERSION': '3.31.6', 'CMAKE_BASE': '/opt/cmake', 'RUSTUP_HOME': '/opt/rust', 'CARGO_HOME': '/opt/rust', 'NINJA_STATUS': '- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ', 'NINJA_HOME': '/opt/ninja', 'NINJA_VERSION': '1.12.1', 'PYENV_ROOT': '/opt/pyenv', 'PYTHON_VERSION': '3.11.15', 'QT_ROOT': '/opt/Qt', 'QT_VERSION': '6.5.3', 'QT_GCC': 'gcc_64', 'QT_MODULES': 'debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES': 'icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=openrv_base_cmake_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.openrv_base_cmake,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"openrv_base_os_build_stage cmds: {cmds}")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_os_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_os_build_stage"])
        ),
    },
)
def openrv_base_ninja_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrv_base_os_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_os_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}

        LABEL org.opencontainers.image.authors="michimussato@etik.com"
        LABEL org.opencontainers.image.source="https://github.com/michimussato/OpenStudioLandscapes-OpenRV-Builder"
        LABEL org.opencontainers.image.vendor="OpenStudioLandscapes"

        SHELL ["/bin/bash", "-c"]

        ARG RV_REPO
        ENV RV_REPO=${{RV_REPO}}

        USER root
        WORKDIR /root

        # static versions:

        # # Install Ninja
        ARG NINJA_STATUS
        # This should persist for Ninja across all child images
        ENV NINJA_STATUS="${{NINJA_STATUS}}"
        ARG NINJA_FLAGS
        ENV NINJA_FLAGS="${{NINJA_FLAGS}}"
        ARG NINJA_HOME
        RUN mkdir -p ${{NINJA_HOME}}
        ARG NINJA_VERSION
        ENV PATH="${{NINJA_HOME}}:${{PATH}}"
        RUN curl \\
                -L https://github.com/ninja-build/ninja/releases/download/v${{NINJA_VERSION}}/ninja-linux.zip \\
                -o ninja-linux.zip \\
            && unzip ninja-linux.zip -d "${{NINJA_HOME}}" \\
            && rm -f ninja-linux.zip

        CMD ["/bin/bash"]
        """).format(
        stage=Targets.openrv_base_ninja,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_ninja_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_ninja_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def openrv_base_ninja_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    openrv_base_ninja_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "RV_REPO": openrvbuilder_resource.RV_REPO,
        # Ninja args
        **{
            "NINJA_STATUS": openrvbuilder_resource.NINJA_STATUS,
            "NINJA_FLAGS": openrvbuilder_resource.NINJA_FLAGS,
            "NINJA_HOME": openrvbuilder_resource.NINJA_HOME,
            "NINJA_VERSION": openrvbuilder_resource.NINJA_VERSION,
        },
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'CMAKE_VERSION': '3.31.6', 'CMAKE_BASE': '/opt/cmake', 'RUSTUP_HOME': '/opt/rust', 'CARGO_HOME': '/opt/rust', 'NINJA_STATUS': '- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ', 'NINJA_HOME': '/opt/ninja', 'NINJA_VERSION': '1.12.1', 'PYENV_ROOT': '/opt/pyenv', 'PYTHON_VERSION': '3.11.15', 'QT_ROOT': '/opt/Qt', 'QT_VERSION': '6.5.3', 'QT_GCC': 'gcc_64', 'QT_MODULES': 'debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES': 'icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=openrv_base_ninja_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.openrv_base_ninja,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"openrv_base_os_build_stage cmds: {cmds}")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_os_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_os_build_stage"])
        ),
    },
)
def openrv_base_pyenv_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrv_base_os_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_os_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}

        LABEL org.opencontainers.image.authors="michimussato@etik.com"
        LABEL org.opencontainers.image.source="https://github.com/michimussato/OpenStudioLandscapes-OpenRV-Builder"
        LABEL org.opencontainers.image.vendor="OpenStudioLandscapes"

        SHELL ["/bin/bash", "-c"]

        ARG RV_REPO
        ENV RV_REPO=${{RV_REPO}}

        USER root
        WORKDIR /root

        # Install pyenv
        ENV PIP_ROOT_USER_ACTION=ignore
        ARG PYENV_ROOT
        ENV PATH="${{PYENV_ROOT}}/shims:${{PYENV_ROOT}}/bin:${{PATH}}"
        RUN git clone http://github.com/pyenv/pyenv.git ${{PYENV_ROOT}}
        RUN echo 'eval "$(pyenv init -)"' >> /etc/profile

        # Python environment
        ARG PYTHON_VERSION
        RUN pyenv install ${{PYTHON_VERSION}}
        RUN pyenv global ${{PYTHON_VERSION}}

        RUN pip install --upgrade pip

        CMD ["/bin/bash"]
        """).format(
        stage=Targets.openrv_base_pyenv,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_pyenv_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_pyenv_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def openrv_base_pyenv_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    vfx_reference_platform_resource: VFXReferencePlatformResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    openrv_base_pyenv_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "RV_REPO": openrvbuilder_resource.RV_REPO,
        **{
            "PYENV_ROOT": openrvbuilder_resource.PYENV_ROOT,
            "PYTHON_VERSION": vfx_reference_platform_resource.get_component.PYTHON,
        },
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'CMAKE_VERSION': '3.31.6', 'CMAKE_BASE': '/opt/cmake', 'RUSTUP_HOME': '/opt/rust', 'CARGO_HOME': '/opt/rust', 'NINJA_STATUS': '- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ', 'NINJA_HOME': '/opt/ninja', 'NINJA_VERSION': '1.12.1', 'PYENV_ROOT': '/opt/pyenv', 'PYTHON_VERSION': '3.11.15', 'QT_ROOT': '/opt/Qt', 'QT_VERSION': '6.5.3', 'QT_GCC': 'gcc_64', 'QT_MODULES': 'debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES': 'icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=openrv_base_pyenv_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.openrv_base_pyenv,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"openrv_base_os_build_stage cmds: {cmds}")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_pyenv_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_pyenv_build_stage"])
        ),
    },
)
def openrv_base_qt_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrv_base_pyenv_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """
    https://aqtinstall.readthedocs.io/en/stable/configuration.html#configuration

    """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_pyenv_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {base_pyenv_image} AS {stage}

        LABEL org.opencontainers.image.authors="michimussato@etik.com"
        LABEL org.opencontainers.image.source="https://github.com/michimussato/OpenStudioLandscapes-OpenRV-Builder"
        LABEL org.opencontainers.image.vendor="OpenStudioLandscapes"

        SHELL ["/bin/bash", "-c"]
        
        # # MERGE IN PYENV
        # ENV PIP_ROOT_USER_ACTION=ignore
        # ARG PYENV_ROOT
        # ENV PATH="${{PYENV_ROOT}}/shims:${{PYENV_ROOT}}/bin:${{PATH}}"
        # COPY --from={{pyenv_image}} ${{PYENV_ROOT}} ${{PYENV_ROOT}}
        # RUN echo 'eval "$(pyenv init -)"' >> /etc/profile

        # ARG RV_REPO
        # ENV RV_REPO=${{RV_REPO}}

        # USER root
        # WORKDIR /root

        # Install Qt
        # This does not seem to cache
        ARG QT_ROOT
        ARG QT_VERSION
        ARG QT_GCC
        ARG QT_MODULES
        ARG QT_ARCHIVES
        ARG QT_HOME
        ENV QT_HOME=${{QT_HOME}}
        RUN python -m pip install aqtinstall
        # To list available aqt installs:
        # - `aqt list-qt linux desktop`
        RUN python -m aqt install-qt linux desktop ${{QT_VERSION}} ${{QT_GCC}} -O ${{QT_ROOT}} \\
            -m ${{QT_MODULES}} \\
            --archives ${{QT_ARCHIVES}}

        CMD ["/bin/bash"]
        """).format(
        stage=Targets.openrv_base_qt,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        base_pyenv_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_qt_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_qt_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def openrv_base_qt_build_stage(
    context: AssetExecutionContext,
    config: QtPackagesConfig,
    vfx_reference_platform_resource: VFXReferencePlatformResource,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    openrv_base_qt_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        **{
            "PYENV_ROOT": openrvbuilder_resource.PYENV_ROOT,
            # "PYTHON_VERSION": CONFIG.PYTHON_VERSION.value,
        },
        # Qt args
        **{
            "QT_ROOT": openrvbuilder_resource.QT_ROOT,
            "QT_VERSION": vfx_reference_platform_resource.get_component.QT,
            "QT_GCC": openrvbuilder_resource.QT_GCC,
            "QT_MODULES": config.qt_modules,
            "QT_ARCHIVES": config.qt_archives,
            "QT_HOME": pathlib.Path(openrvbuilder_resource.QT_ROOT).joinpath(vfx_reference_platform_resource.get_component.QT, openrvbuilder_resource.QT_GCC).as_posix(),
        },
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'CMAKE_VERSION': '3.31.6', 'CMAKE_BASE': '/opt/cmake', 'RUSTUP_HOME': '/opt/rust', 'CARGO_HOME': '/opt/rust', 'NINJA_STATUS': '- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ', 'NINJA_HOME': '/opt/ninja', 'NINJA_VERSION': '1.12.1', 'PYENV_ROOT': '/opt/pyenv', 'PYTHON_VERSION': '3.11.15', 'QT_ROOT': '/opt/Qt', 'QT_VERSION': '6.5.3', 'QT_GCC': 'gcc_64', 'QT_MODULES': 'debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES': 'icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=openrv_base_qt_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.openrv_base_qt,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"openrv_base_os_build_stage cmds: {cmds}")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_os_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_os_build_stage"])
        ),
        "openrv_base_rust_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_rust_build_stage"])
        ),
        "openrv_base_cmake_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_cmake_build_stage"])
        ),
        "openrv_base_ninja_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_ninja_build_stage"])
        ),
        "openrv_base_qt_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_qt_build_stage"])
        ),
    },
)
def openrv_base_comp_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrv_base_os_build_stage: Dict,
    openrv_base_rust_build_stage: Dict,
    openrv_base_cmake_build_stage: Dict,
    openrv_base_ninja_build_stage: Dict,
    openrv_base_qt_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################
    # BASE OS

    (
        build_base_os_image_name,
        build_base_os_image_prefixes,
        build_base_os_tags,
        build_base_os_parent_image_prefix,
        build_base_os_parent_image_name,
        build_base_os_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_os_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    #################################################
    # RUST

    (
        build_base_rust_image_name,
        build_base_rust_image_prefixes,
        build_base_rust_tags,
        build_base_rust_parent_image_prefix,
        build_base_rust_parent_image_name,
        build_base_rust_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_rust_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    #################################################
    # CMAKE

    (
        build_base_cmake_image_name,
        build_base_cmake_image_prefixes,
        build_base_cmake_tags,
        build_base_cmake_parent_image_prefix,
        build_base_cmake_parent_image_name,
        build_base_cmake_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_cmake_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    #################################################
    # NINJA

    (
        build_base_ninja_image_name,
        build_base_ninja_image_prefixes,
        build_base_ninja_tags,
        build_base_ninja_parent_image_prefix,
        build_base_ninja_parent_image_name,
        build_base_ninja_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_ninja_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    #################################################
    # QT

    (
        build_base_qt_image_name,
        build_base_qt_image_prefixes,
        build_base_qt_tags,
        build_base_qt_parent_image_prefix,
        build_base_qt_parent_image_name,
        build_base_qt_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_qt_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {base_os_image} AS {stage}

        LABEL org.opencontainers.image.authors="michimussato@etik.com"
        LABEL org.opencontainers.image.source="https://github.com/michimussato/OpenStudioLandscapes-OpenRV-Builder"
        LABEL org.opencontainers.image.vendor="OpenStudioLandscapes"

        SHELL ["/bin/bash", "-c"]

        ARG RV_REPO
        ENV RV_REPO=${{RV_REPO}}

        USER root
        WORKDIR /root
        
        # MERGE IN RUST
        # can we re-use the already defined env vars here? 
        # probably not but not verified.
        ARG RUSTUP_HOME
        ENV RUSTUP_HOME=${{RUSTUP_HOME}}
        ARG CARGO_HOME
        ENV CARGO_HOME=${{CARGO_HOME}}
        ENV PATH="${{RUSTUP_HOME}}/bin:${{CARGO_HOME}}/bin:${{PATH}}"
        COPY --from={rust_image} ${{RUSTUP_HOME}} ${{RUSTUP_HOME}}
        
        # MERGE IN CMAKE
        ARG CMAKE_VERSION
        ARG CMAKE_BASE
        ENV PATH="${{CMAKE_BASE}}/bin:${{PATH}}"
        COPY --from={cmake_image} ${{CMAKE_BASE}} ${{CMAKE_BASE}}
        
        # MERGE IN NINJA
        ARG NINJA_STATUS
        # This should persist for Ninja across all child images
        ENV NINJA_STATUS="${{NINJA_STATUS}}"
        ARG NINJA_FLAGS
        ENV NINJA_FLAGS="${{NINJA_FLAGS}}"
        ARG NINJA_HOME
        # RUN mkdir -p ${{NINJA_HOME}}
        ARG NINJA_VERSION
        ENV PATH="${{NINJA_HOME}}:${{PATH}}"
        COPY --from={ninja_image} ${{NINJA_HOME}} ${{NINJA_HOME}}
        
        # MERGE IN QT
        ARG QT_HOME
        ENV QT_HOME=${{QT_HOME}}
        COPY --from={qt_image} ${{QT_HOME}} ${{QT_HOME}}

        CMD ["/bin/bash"]
        """).format(
        stage=Targets.openrv_base_comp,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=build_base_os_image_name,
        base_os_image=f"{build_base_os_parent_image_prefix}{build_base_os_parent_image_name}:{build_base_os_parent_image_tags[0]}",
        rust_image=f"{build_base_rust_parent_image_prefix}{build_base_rust_parent_image_name}:{build_base_rust_parent_image_tags[0]}",
        cmake_image=f"{build_base_cmake_parent_image_prefix}{build_base_cmake_parent_image_name}:{build_base_cmake_parent_image_tags[0]}",
        ninja_image=f"{build_base_ninja_parent_image_prefix}{build_base_ninja_parent_image_name}:{build_base_ninja_parent_image_tags[0]}",
        qt_image=f"{build_base_qt_parent_image_prefix}{build_base_qt_parent_image_name}:{build_base_qt_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BASE_OS,
    ins={
        "openrv_base_comp_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_comp_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def openrv_base_comp_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    vfx_reference_platform_resource: VFXReferencePlatformResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    openrv_base_comp_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "RV_REPO": openrvbuilder_resource.RV_REPO,
        # Cmake args
        **{
            "CMAKE_BASE": openrvbuilder_resource.CMAKE_BASE,
        },
        # Rust args
        **{
            "RUSTUP_HOME": openrvbuilder_resource.RUSTUP_HOME,
            "CARGO_HOME": openrvbuilder_resource.CARGO_HOME,
        },
        # Ninja args
        **{
            "NINJA_STATUS": openrvbuilder_resource.NINJA_STATUS,
            "NINJA_FLAGS": openrvbuilder_resource.NINJA_FLAGS,
            "NINJA_HOME": openrvbuilder_resource.NINJA_HOME,
            "NINJA_VERSION": openrvbuilder_resource.NINJA_VERSION,
        },
        **{
            "QT_HOME": pathlib.Path(
                openrvbuilder_resource.QT_ROOT
            )
            .joinpath(
                vfx_reference_platform_resource.get_component.QT,
                openrvbuilder_resource.QT_GCC
            ).as_posix(),
        },
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'CMAKE_VERSION': '3.31.6', 'CMAKE_BASE': '/opt/cmake', 'RUSTUP_HOME': '/opt/rust', 'CARGO_HOME': '/opt/rust', 'NINJA_STATUS': '- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ', 'NINJA_HOME': '/opt/ninja', 'NINJA_VERSION': '1.12.1', 'PYENV_ROOT': '/opt/pyenv', 'PYTHON_VERSION': '3.11.15', 'QT_ROOT': '/opt/Qt', 'QT_VERSION': '6.5.3', 'QT_GCC': 'gcc_64', 'QT_MODULES': 'debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES': 'icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=openrv_base_comp_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.openrv_base_comp,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"openrv_base_os_build_stage cmds: {cmds}")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "openrv_base_comp_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_comp_build_stage"])
        ),
    },
)
def rv_clone_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    config: CloneRepoAssetConfig,
    openrv_base_comp_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_comp_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}

        WORKDIR ${{RV_REPO}}

        # Build OpenRV
        RUN git clone --recursive https://github.com/AcademySoftwareFoundation/OpenRV.git . \\
            && git checkout {branch} \\
            && echo "Cache busted at {cache_busted}" \\
            && echo "Current commit: $(git rev-parse --short HEAD)"
        RUN git config blame.ignoreRevsFile .git-blame-ignore-revs
        RUN git config --global --add safe.directory ${{RV_REPO}}

        # Inject OpenStudioLandscapes hint
        # File to edit ${{RV_REPO}}/src/lib/app/RvCommon/generate_about_rv.py
        RUN sed -i 's/    html_content = \[\]/    html_content = \["<p>", "<b>", "OpenStudioLandscapes Edition", "<\/b>", "<\/p>", "<p>", "Happily brought to you by OpenStudioLandscapes (built with OpenStudioLandscapes-OpenRV-Builder)", "<\/p>"\]/' ${{RV_REPO}}/src/lib/app/RvCommon/generate_about_rv.py

        CMD ["/bin/bash"]
        """).format(
        branch=config.commit,  # or CONFIG.branch,
        cache_busted=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        stage=Targets.rv_clone,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        # Todo: this won't work as expected if len(tags) > 1
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_clone_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_clone_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def rv_clone_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    rv_clone_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=rv_clone_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.rv_clone,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"{cmds = }")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "check_commit_hash": MetadataValue.path("git rev-parse HEAD"),
            "check_commit_hash_short": MetadataValue.path("git rev-parse --short HEAD"),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_clone_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_clone_build_stage"])
        ),
    },
)
def rv_configure_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrv_codecs_resource: OpenRVCodecsResource,
    rv_clone_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=rv_clone_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # CONFIG = Feature(['env={}', 'local_bind_volumes=[]', 'local_environment_variables={}', 'config_engine=None', 'distribution=None', 'group_name=OpenStudioLandscapes_OpenRV_Builder', "key_prefixes=['OpenStudioLandscapes_OpenRV_Builder']", 'enabled=True', 'compose_scope=default', 'feature_name=OpenStudioLandscapes-OpenRV-Builder', 'docker_compose={DOT_LANDSCAPES}/{LANDSCAPE}/{FEATURE}/docker_compose/docker-compose.yml', 'apptainer_agent=docker-daemon', "RV_FFMPEG_NON_FREE_DECODERS_TO_ENABLE=[<DecodersFFMPEG.AAC: 'aac'>, <DecodersFFMPEG.HEVC: 'hevc'>, <DecodersFFMPEG.DNXHD: 'dnxhd'>]", "RV_FFMPEG_NON_FREE_ENCODERS_TO_ENABLE=[<EncodersFFMPEG.AAC: 'aac'>, <EncodersFFMPEG.DNXHD: 'dnxhd'>]", 'RV_DEPS_BMD_DECKLINK_SDK_ZIP_PATH=', 'RV_DEPS_APPLE_PRORES_SDK_ZIP_PATH=', 'NDI_SDK_ROOT=', 'branch=main', 'RV_BUILD_PARALLELISM=8', 'RV_REPO=/.rv/git/OpenRV', 'RV_HOME=/.rv/git/OpenRV/_build/stage/app', 'RV_INSTALL_DIR=/rv', 'RV_INST_DIR=/rv', 'RV_BUILD_DIR=_build', 'RV_STAGE_DIR=/.rv/git/OpenRV/_build/stage/app', 'rv_build_dir=/.rv', 'override_enabled_docker_cache=True', 'RV_BUILD_TYPE=Release', 'RV_VFX_PLATFORM=CY2024', 'CMAKE_GENERATOR=Ninja', 'CMAKE_BASE=/opt/cmake', 'CMAKE_VERSION=3.31.6', 'RUSTUP_HOME=/opt/rust', 'CARGO_HOME=/opt/rust', 'NINJA_STATUS=- Ninja %p [%w] [%f/%t] - ', 'NINJA_HOME=/opt/ninja', 'NINJA_VERSION=1.12.1', 'NINJA_FLAGS=--verbose -k 0', 'PYENV_ROOT=/opt/pyenv', 'PYTHON_VERSION=3.11.15', 'QT_MODULES=debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES=icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland', 'QT_ROOT=/opt/Qt', 'QT_VERSION=6.5.3', 'QT_GCC=gcc_64'])
    context.log.info(openrv_codecs_resource.rv_ffmpeg_non_free_decoders_to_enable)
    context.log.info(openrv_codecs_resource.rv_ffmpeg_non_free_encoders_to_enable)

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}
        # 1. Configure
        #
        # Blueprint:
        # - https://github.com/AcademySoftwareFoundation/OpenRV/blob/3dad8d05a3cd8b02c6ef86983cc9291cc01fbd8b/.github/actions/build-linux/action.yml#L267
        
        ARG RV_BUILD_DIR
        ARG CMAKE_GENERATOR
        ARG RV_TOOLCHAIN
        ARG RV_BUILD_TYPE
        ARG RV_VFX_PLATFORM
        
        WORKDIR ${{RV_REPO}}
        
        # trace result: /.rv/git/OpenRV/trace_{stage}.log
        # docker exec -it <ID> tail -f /.rv/git/OpenRV/trace_{stage}.log
        RUN python3 -m venv .venv \\
            && source .venv/bin/activate \\
            && pip install --upgrade pip \\
            && SETUPTOOLS_USE_DISTUTILS=${{SETUPTOOLS_USE_DISTUTILS}} \\
                python3 -m pip install --upgrade -r ${{RV_REPO}}/requirements.txt \\
            && pre-commit install \\
            && cmake \\
                --graphviz=./graphviz_{stage}.dot \\
                --trace-expand \\
                --trace-redirect ./trace_{stage}.log \\
                -B ${{RV_BUILD_DIR}} \\
                -G "${{CMAKE_GENERATOR}}" \\
                ${{RV_TOOLCHAIN}} \\
                ${{CMAKE_WIN_ARCH}} \\
                -DCMAKE_BUILD_TYPE=${{RV_BUILD_TYPE}} \\
                -DRV_DEPS_QT_LOCATION=${{QT_HOME}} \\
                -DRV_VFX_PLATFORM=${{RV_VFX_PLATFORM}} \\
                -DRV_DEPS_WIN_PERL_ROOT=${{WIN_PERL}} \\
                -DRV_FFMPEG_NON_FREE_DECODERS_TO_ENABLE="{RV_FFMPEG_NON_FREE_DECODERS_TO_ENABLE}" \\
                -DRV_FFMPEG_NON_FREE_ENCODERS_TO_ENABLE="{RV_FFMPEG_NON_FREE_ENCODERS_TO_ENABLE}" \\
                -DRV_DEPS_BMD_DECKLINK_SDK_ZIP_PATH={RV_DEPS_BMD_DECKLINK_SDK_ZIP_PATH} \\
                -DRV_DEPS_APPLE_PRORES_SDK_ZIP_PATH={RV_DEPS_APPLE_PRORES_SDK_ZIP_PATH} \\
                -DNDI_SDK_ROOT={NDI_SDK_ROOT} \\
                --log-level=trace \\
            && deactivate
        """).format(
        stage=Targets.rv_configure,
        timezone=auto_builder_resource.tz,
        RV_FFMPEG_NON_FREE_DECODERS_TO_ENABLE=openrv_codecs_resource.rv_ffmpeg_non_free_decoders_to_enable,
        RV_FFMPEG_NON_FREE_ENCODERS_TO_ENABLE=openrv_codecs_resource.rv_ffmpeg_non_free_encoders_to_enable,
        # OPTIONAL_SDK_ROOT=openrvbuilder_resource.OPTIONAL_SDK_ROOT,
        RV_DEPS_BMD_DECKLINK_SDK_ZIP_PATH=openrvbuilder_resource.RV_DEPS_BMD_DECKLINK_SDK_ZIP_PATH,
        RV_DEPS_APPLE_PRORES_SDK_ZIP_PATH=openrvbuilder_resource.RV_DEPS_APPLE_PRORES_SDK_ZIP_PATH,
        NDI_SDK_ROOT=openrvbuilder_resource.NDI_SDK_ROOT,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        # Todo: this won't work as expected if len(tags) > 1
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "rv_configure_build_stage_trace_log": MetadataValue.path(f"docker exec -it <ID> tail -f /.rv/git/OpenRV/trace_{Targets.rv_configure}.log"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_configure_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_configure_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def rv_configure_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    vfx_reference_platform_resource: VFXReferencePlatformResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    rv_configure_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "RV_VFX_PLATFORM": vfx_reference_platform_resource.get_component.CY,
        "RV_BUILD_DIR": openrvbuilder_resource.RV_BUILD_DIR,
        "CMAKE_GENERATOR": openrvbuilder_resource.CMAKE_GENERATOR.value,
        "RV_TOOLCHAIN": RVToolChain.LINUX,
        "RV_BUILD_TYPE": auto_builder_resource.build_type,
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'RV_VFX_PLATFORM': 'CY2024', 'RV_BUILD_PARALLELISM': '8', 'RV_HOME': '/.rv/git/OpenRV', 'RV_BUILD_DIR': '/.rv/git/OpenRV/_build', 'RV_INST_DIR': '/.rv/git/OpenRV/_install', 'CMAKE_GENERATOR': 'Ninja', 'QT_HOME': '/opt/Qt/6.5.3/gcc_64'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=rv_configure_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.rv_configure,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"{cmds = }")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "download_trace": MetadataValue.path(
                f"docker cp $(docker create --name tc {image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}):/.rv/git/OpenRV/trace_{Targets.rv_configure}.log ./trace_{Targets.rv_configure}_{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}.log && docker rm tc"
            ),
            "download_dot": MetadataValue.path(
                f"docker cp $(docker create --name tc {image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}):/.rv/git/OpenRV/graphviz_{Targets.rv_configure}.dot ./graphviz_{Targets.rv_configure}_{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}.dot && docker rm tc && dot -Tpng -o ./graphviz_{Targets.rv_configure}_{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}.png ./graphviz_{Targets.rv_configure}_{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}.dot"
            ),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_configure_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_configure_build_stage"])
        ),
    },
)
def rv_dependencies_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    rv_configure_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=rv_configure_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # Todo:
    #  - [ ]  Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv
    #         ENV PIP_ROOT_USER_ACTION=ignore

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}
        # 2. Build Third Party Dependencies
        # RUN source rvcmds.sh \\
        #     && rvbuildt dependencies
        
        ARG TARGET
        ARG NINJA_FLAGS
        ARG RV_BUILD_DIR
        ARG RV_BUILD_TYPE
        ARG RV_BUILD_PARALLELISM
        
        WORKDIR ${{RV_REPO}}
        
        RUN source .venv/bin/activate \\
            && set -x \\
            && cmake \\
                --build ${{RV_BUILD_DIR}} \\
                --config ${{RV_BUILD_TYPE}} \\
                -v \\
                --parallel=${{RV_BUILD_PARALLELISM}} \\
                --target ${{TARGET}} \\
                -- ${{NINJA_FLAGS}} \\
            && set +x \\
            && deactivate
        """).format(
        stage=Targets.rv_dependencies,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        # Todo: this won't work as expected if len(tags) > 1
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_dependencies_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_dependencies_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def rv_dependencies_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    rv_dependencies_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:

    """
    stderr: #7 ERROR: failed to extract layer sha256:8a54ee66950cf86433dace97e08f510c12d627b69e2a6e1707eff7b5d82aa5a5: failed call to UtimesNanoAt for /data/local/var_lib_containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/11854/fs/.rv/git/OpenRV/_build/RV_DEPS_AJA/build/CMakeFiles: no such file or directory
    stderr: ERROR: failed to build: failed to solve: failed to extract layer sha256:8a54ee66950cf86433dace97e08f510c12d627b69e2a6e1707eff7b5d82aa5a5: failed call to UtimesNanoAt for /data/local/var_lib_containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/11854/fs/.rv/git/OpenRV/_build/RV_DEPS_AJA/build/CMakeFiles: no such file or directory
    """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "TARGET": RVBuildTarget.DEPENDENCIES.value,
        # Todo
        #  - [ ] remove
        "NINJA_FLAGS": openrvbuilder_resource.NINJA_FLAGS,
        "RV_BUILD_DIR": openrvbuilder_resource.RV_BUILD_DIR,
        "RV_BUILD_TYPE": auto_builder_resource.build_type,
        "RV_BUILD_PARALLELISM": str(openrvbuilder_resource.RV_BUILD_PARALLELISM),
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'RV_HOME': '/.rv/git/OpenRV', 'RV_BUILD_DIR': '/.rv/git/OpenRV/_build', 'CMAKE_GENERATOR': 'Ninja', 'RV_BUILD_PARALLELISM': '8', 'RV_BUILD_TYPE': 'Release', 'TARGET': 'dependencies', 'NINJA_FLAGS': '--verbose -k 0', 'RV_VFX_PLATFORM': 'CY2024', 'QT_HOME': '/opt/Qt/6.5.3/gcc_64', 'RV_INST_DIR': '/.rv/git/OpenRV/_install'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=rv_dependencies_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.rv_dependencies,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"{cmds = }")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    # try:
    logs = docker_do(
        context=context,
        cmds=cmds,
    )
    # except OpenStudioLandscapesStreamingProcessException:
    #     context.log.exception()

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_dependencies_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_dependencies_build_stage"])
        ),
    },
)
def rv_build_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    rv_dependencies_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=rv_dependencies_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}
        # 3. Build OpenRV
        # RUN source rvcmds.sh \\
        #     && rvbuildt main_executable  # or `rvbuild`
        
        ARG TARGET
        ARG NINJA_FLAGS
        ARG RV_BUILD_DIR
        ARG RV_BUILD_TYPE
        ARG RV_BUILD_PARALLELISM
        
        WORKDIR ${{RV_REPO}}
        
        RUN source .venv/bin/activate \\
            && set -x \\
            && cmake \\
                --build ${{RV_BUILD_DIR}} \\
                --config ${{RV_BUILD_TYPE}} \\
                -v \\
                --parallel=${{RV_BUILD_PARALLELISM}} \\
                --target ${{TARGET}} \\
                -- ${{NINJA_FLAGS}} \\
            && set +x \\
            && deactivate
        """).format(
        stage=Targets.rv_build,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        # Todo: this won't work as expected if len(tags) > 1
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_build_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_build_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def rv_build_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    rv_build_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """
    Known Issues

    - [output clipped, log limit 2MiB reached]
    """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "TARGET": RVBuildTarget.MAIN_EXECUTABLE.value,
        "NINJA_FLAGS": openrvbuilder_resource.NINJA_FLAGS,
        "RV_BUILD_DIR": openrvbuilder_resource.RV_BUILD_DIR,
        "RV_BUILD_TYPE": auto_builder_resource.build_type,
        "RV_BUILD_PARALLELISM": str(openrvbuilder_resource.RV_BUILD_PARALLELISM),
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'RV_VFX_PLATFORM': 'CY2024', 'RV_BUILD_PARALLELISM': '8', 'RV_HOME': '/.rv/git/OpenRV', 'RV_BUILD_DIR': '/.rv/git/OpenRV/_build', 'RV_INST_DIR': '/.rv/git/OpenRV/_install', 'CMAKE_GENERATOR': 'Ninja', 'QT_HOME': '/opt/Qt/6.5.3/gcc_64'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=rv_build_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.rv_build,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"{cmds = }")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_build_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_build_build_stage"])
        ),
    },
)
def rv_install_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    rv_build_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=rv_build_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}
        # 4. Install OpenRV
        # RUN source rvcmds.sh \
        #     && rvinst
        
        ARG RV_BUILD_DIR
        ARG RV_INST_DIR
        ARG RV_BUILD_TYPE
        
        WORKDIR ${{RV_REPO}}
        
        # `--prefix {{rv_install_dir}}` hardcoded for now because of https://github.com/AcademySoftwareFoundation/OpenRV/issues/1322
        # Should be `--prefix ${{RV_INST_DIR}}`
        # maybe try readonly?
        # export RV_INST_DIR=/rv
        # readonly RV_INST_DIR
        RUN source .venv/bin/activate \
            && cmake \\
                --install ${{RV_BUILD_DIR}} \\
                --prefix ${{RV_INST_DIR}} \\
                --config ${{RV_BUILD_TYPE}} \
            && deactivate
        """).format(
        stage=Targets.rv_install,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=image_name,
        # Todo: this won't work as expected if len(tags) > 1
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_install_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_install_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def rv_install_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    rv_install_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    # just highlight the message
    context.log.debug(f"{image_data = }")

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "RV_BUILD_DIR": openrvbuilder_resource.RV_BUILD_DIR,
        "RV_INST_DIR": openrvbuilder_resource.RV_INST_DIR,
        "RV_BUILD_TYPE": auto_builder_resource.build_type,
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'CMAKE_VERSION': '3.31.6', 'CMAKE_BASE': '/opt/cmake', 'RUSTUP_HOME': '/opt/rust', 'CARGO_HOME': '/opt/rust', 'NINJA_STATUS': '- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ', 'NINJA_HOME': '/opt/ninja', 'NINJA_VERSION': '1.12.1', 'PYENV_ROOT': '/opt/pyenv', 'PYTHON_VERSION': '3.11.15', 'QT_ROOT': '/opt/Qt', 'QT_VERSION': '6.5.3', 'QT_GCC': 'gcc_64', 'QT_MODULES': 'debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES': 'icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=rv_install_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.rv_install,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"{cmds = }")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "openrv_base_os_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_os_build_stage"])
        ),
        "rv_install_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_install_build_stage"])
        ),
    },
)
def rv_install_clean_write_dockerfile(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    openrv_base_os_build_stage: Dict,
    rv_install_build_stage: Dict,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    docker_file = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    shutil.rmtree(docker_file.parent, ignore_errors=True)

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    # Base

    (
        base_image_name,
        base_image_prefixes,
        base_tags,
        base_build_base_parent_image_prefix,
        base_build_base_parent_image_name,
        base_build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=openrv_base_os_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    #################################################

    # Build

    (
        build_image_name,
        build_image_prefixes,
        build_tags,
        build_build_base_parent_image_prefix,
        build_build_base_parent_image_name,
        build_build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=rv_install_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        FROM {parent_image} AS {stage}
        
        ARG RV_INST_DIR
        
        COPY --from={build_image} ${{RV_INST_DIR}} ${{RV_INST_DIR}}
        
        WORKDIR ${{RV_INST_DIR}}
        
        CMD ["/bin/bash"]
        """).format(
        stage=Targets.rv_install_clean,
        timezone=auto_builder_resource.tz,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        image_name=base_image_name,
        # Todo: this won't work as expected if len(tags) > 1
        parent_image=f"{base_build_base_parent_image_prefix}{base_build_base_parent_image_name}:{base_build_base_parent_image_tags[0]}",
        build_image=f"{build_build_base_parent_image_prefix}{build_build_base_parent_image_name}:{build_build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(docker_file, mode="w", encoding="utf-8") as fw:
        fw.write(docker_file_str)

    with open(docker_file, mode="r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


# noinspection PyDeprecation
@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_install_clean_write_dockerfile": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_install_clean_write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def rv_install_clean_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    vfx_reference_platform_resource: VFXReferencePlatformResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    rv_install_clean_write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config_resource,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        context.dagster_run.run_id,
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context.log.debug(f"{image_data = }")
    # image_data = {
    #     'image_name': 'openstudiolandscapes_openrv_builder_rv_install_clean_build_stage',
    #     'image_prefixes': 'registry.openstudiolandscapes.lan:5000/openstudiolandscapes/',
    #     'image_tags': ['2026-07-01_20-41-40__deeply-shimmer-trusted-honeycup'],
    #     'image_parent': {}
    # }

    cmds = []

    # tags_local = [f"{image_prefix_local}{image_name}:{tag}" for tag in tags]
    tags_full_str = [f"{image_prefixes}{image_name}:{tag}" for tag in tags]
    context.log.debug(f"{tags_full_str = }")

    build_args = {
        "RV_INST_DIR": openrvbuilder_resource.RV_INST_DIR,
    }

    context.log.debug(f"{build_args = }")
    # build_args = {'CMAKE_VERSION': '3.31.6', 'CMAKE_BASE': '/opt/cmake', 'RUSTUP_HOME': '/opt/rust', 'CARGO_HOME': '/opt/rust', 'NINJA_STATUS': '- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ', 'NINJA_HOME': '/opt/ninja', 'NINJA_VERSION': '1.12.1', 'PYENV_ROOT': '/opt/pyenv', 'PYTHON_VERSION': '3.11.15', 'QT_ROOT': '/opt/Qt', 'QT_VERSION': '6.5.3', 'QT_GCC': 'gcc_64', 'QT_MODULES': 'debug_info qt3d qt5compat qtcharts qtconnectivity qtdatavis3d qtgrpc qthttpserver qtimageformats qtlanguageserver qtlocation qtlottie qtmultimedia qtnetworkauth qtpdf qtpositioning qtquick3d qtquick3dphysics qtquickeffectmaker qtquicktimeline qtremoteobjects qtscxml qtsensors qtserialbus qtserialport qtshadertools qtspeech qtvirtualkeyboard qtwaylandcompositor qtwebchannel qtwebengine qtwebsockets qtwebview', 'QT_ARCHIVES': 'icu qtbase qtdeclarative qtsvg qttools qttranslations qtwayland'}

    cmd_build = docker_build_cmd(
        context=context,
        docker_config_json=docker_config_resource.docker_config_json_root,
        docker_file=rv_install_clean_write_dockerfile,
        tags=tags_full_str,
        pull=docker_config_resource.docker_pull,
        target=Targets.rv_install_clean,
        no_cache=auto_builder_resource.override_enabled_docker_cache or docker_config_resource.no_cache,
        build_args=build_args,
    )

    cmds.append(cmd_build)

    if all(
        [
            docker_config_resource.use_registry,
            docker_config_resource.docker_push,
        ]
    ):  # or not_push
        cmds_push = docker_push_cmd(
            context=context,
            docker_config_json=docker_config_resource.docker_config_json_root,
            tags_full=tags_full_str,
        )

        cmds.extend(cmds_push)
    else:
        pass

    context.log.info(f"{cmds = }")
    # cmds = [['/usr/local/bin/docker', '--debug', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'build', '--progress', 'plain', '--pull', '--file', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles/Dockerfile', '--no-cache', '--tag', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__build_docker_image/Dockerfiles'], ['/usr/local/bin/docker', '--config', '/home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json', 'push', 'openstudiolandscapes_base_build_docker_image:2025-11-16-17-38-11-6545bb6740ab406189bad0aa0820844f']]

    logs = docker_do(
        context=context,
        cmds=cmds,
    )

    # Run ldd and save output to Materialization
    cmd_ldd = []

    docker_run_cmd = [
        shutil.which("docker"),
        # "--debug",
        f"--config={docker_config_resource.docker_config_json_root.as_posix()}",
        "run",
        "--shm-size=32g",
        "--rm",
        # "--user=$(id -u):$(id -g)",
        "--interactive",
        "--tty",
        # f"--volume={host_mount.as_posix()}:{tar_root.as_posix()}:rw",
        f"--name=OpenStudioLandscapes-ASWF-OpenRV-BuildBox-{vfx_reference_platform_resource.get_component.CY}-get-ldd-info",
        f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}",
    ]

    cmd_ldd.extend(
        docker_run_cmd,
    )

    cmd_ldd.extend(
        [
            "/usr/bin/ldd",
            "--verbose",
            "/rv/bin/rv.bin",
        ]
    )

    ret_ldd = subprocess.run(
        cmd_ldd,
        stdout=subprocess.PIPE,
    ).stdout.decode("utf-8")  # .strip()

    context.log.debug(f"{ret_ldd = }")

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_build_cmd": MetadataValue.path(" ".join(cmd_build["cmd"])),
            "docker_run_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
            "ret_ldd": MetadataValue.md(f"```shell\n{ret_ldd}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_TARBALL,
    ins={
        "build_name": AssetIn(AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "build_name"])),
        "rv_install_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_install_clean_build_stage"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def rv_tar_xz_build_stage(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    vfx_reference_platform_resource: VFXReferencePlatformResource,
    build_name: str,  # pylint: disable=redefined-outer-name
    rv_install_build_stage: Dict,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    host_mount = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "tar",
        "runs",
        context.dagster_run.run_id,
    )

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=rv_install_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    tar_root = pathlib.Path(
        "/tarballs",
    )

    # tar_archive_name: str = f"{build_name}__{datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}.tar.xz"
    tar_archive_name: str = f"{build_name}.tar.xz"

    tar_path = tar_root.joinpath(tar_archive_name)

    exclude_from_quote.extend(
        [
            tar_path.as_posix(),
        ]
    )

    cmd = []

    host_mount.mkdir(parents=True, exist_ok=True)

    docker_run_cmd = [
        shutil.which("docker"),
        "--debug",
        f"--config={docker_config_resource.docker_config_json_root.as_posix()}",
        "run",
        "--shm-size=32g",
        # "--env", "HOST_UID=${UID}",
        # "--env", "HOST_UID=$(id -u)",
        # "--env", "HOST_GID=${GID}",
        # "--env", "HOST_GID=$(id -g)",
        "--rm",
        # "--user=$(id -u):$(id -g)",
        "--interactive",
        # --tty is not allowed if OpenStudioLandscapes-OpenRV-Builder runs as a Systemd unit.
        # is `--tty` necessary at all?
        # -> works outside of systemd without --tty
        # "--tty",
        f"--volume={host_mount.as_posix()}:{tar_root.as_posix()}:rw",
        f"--name=OpenStudioLandscapes-ASWF-OpenRV-BuildBox-{vfx_reference_platform_resource.get_component.CY}-{Targets.rv_tar_xz}",
        f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    ]

    cmd.extend(
        docker_run_cmd,
    )

    # exclude_from_quote.extend(
    #     [
    #         "HOST_UID=$(id -u)",
    #         "HOST_GID=$(id -g)",
    #     ]
    # )

    docker_run_cmd_str = " ".join(shlex.quote(s) if not s in exclude_from_quote else s for s in docker_run_cmd)

    container_cmds = []

    tar_cmd = [
        "source",
        "/etc/os-release",
        "&&",
        "tar",
        "-C",
        "/rv",
        # CONFIG.RV_INST_DIR.as_posix(),
        "--create",
        "--verbose",
        # https://billauer.se/blog/2020/11/tar-create-owner-group/
        "--owner=0",
        "--group=0",
        "--mode='og-w'",
        "--file",
        "-",
        ".",
        "|",
        "xz",
        "--verbose",
        "--threads=0",
        "-9",
        "--stdout",
        "-",
        ">",
        tar_path.as_posix(),
    ]

    container_cmds.extend(tar_cmd)

    # # Todo
    # #  - [ ] Does not work yet in StreamingProcess (disabled for now):
    # #        stdout: chown: invalid group: ':${GID}'
    # #  - [ ] stdout: chown: invalid option -- 'u'
    # chown_cmd = [
    #     "&&",
    #     "chown",
    #     # chown-ing with id needs +
    #     # Does not work:
    #     # "+${HOST_UID}:+${HOST_GID}",
    #     # works:
    #     "+1000:+1001",
    #     tar_path.as_posix(),
    # ]
    #
    # exclude_from_quote.extend(
    #     [
    #         "+${HOST_UID}:+${HOST_GID}",
    #     ]
    # )
    #
    # container_cmds.extend(chown_cmd)

    chmod_cmd = [
        "&&",
        "chmod",
        "a+rw",
        tar_path.as_posix(),
    ]

    container_cmds.extend(chmod_cmd)

    stat_cmd = [
        "&&",
        "stat",
        tar_path.as_posix(),
    ]

    container_cmds.extend(stat_cmd)

    tar_test_cmd = [
        "&&",
        "tar",
        "--verbose",
        "--list",
        "--file",
        tar_path.as_posix(),
        ">",
        "/dev/null",
    ]

    container_cmds.extend(tar_test_cmd)

    exit_cmd = [
        "&&",
        "exit",
        "0",
    ]

    container_cmds.extend(exit_cmd)

    container_cmds_str = " ".join(shlex.quote(s) if not s in exclude_from_quote else s for s in container_cmds)

    cmd.extend(
        [
            "/bin/bash",
            "-c",
            container_cmds_str,
            # f'"{container_cmds_str}"',
        ]
    )

    cmds = [
        {
            "cmd": cmd,
            "env": {},
        }
    ]

    context.log.info(f"{cmds = }")
    # cmds = [{'cmd': ['/usr/local/bin/docker', '--debug', '--config=/home/michael/.local/share/OpenStudioLandscapes/.landscapes/2026-07-08_09-36-52__political-confusion-marred-motorist/OpenStudioLandscapes/OpenStudioLandscapes_Base__docker_config_json', 'run', '--shm-size=32g', '--env', 'HOST_UID=$(id -u)', '--env', 'HOST_GID=$(id -g)', '--rm', '--interactive', '--tty', '--volume=/home/michael/.local/share/OpenStudioLandscapes/.landscapes/OpenStudioLandscapes-OpenRV-Builder/OpenStudioLandscapes_OpenRV_Builder__rv_tar_xz_build_stage/tar/runs/3dc15662-ae23-43df-952d-3c0dd964a3b2:/tarballs:rw', '--name=OpenStudioLandscapes-ASWF-OpenRV-BuildBox-CY2024-rv_tar_xz', 'openstudiolandscapes_openrv_builder_rv_install_clean_build_stage:2026-07-08_09-36-52__political-confusion-marred-motorist', '/bin/bash', '-c', "source /etc/os-release && tar -C /rv --create --verbose --owner=0 --group=0 --mode='og-w' --file - . | xz --verbose --threads=0 -9 --stdout - > /tarballs/OpenRV-4.0.0-51a2eb21-CY2024-release-Rocky-Linux-9-x86_64.tar.xz && chown ${HOST_UID}:${HOST_GID} /tarballs/OpenRV-4.0.0-51a2eb21-CY2024-release-Rocky-Linux-9-x86_64.tar.xz && tar --verbose --list --file /tarballs/OpenRV-4.0.0-51a2eb21-CY2024-release-Rocky-Linux-9-x86_64.tar.xz > /dev/null"], 'env': {}}]
    # /usr/local/bin/docker --debug --config=/home/michael/.local/share/OpenStudioLandscapes/.landscapes/2026-07-08_09-36-52__political-confusion-marred-motorist/OpenStudioLandscapes/OpenStudioLandscapes_Base__docker_config_json run --shm-size=32g --env HOST_UID=$(id -u) --env HOST_GID=$(id -g) --rm --interactive --tty --volume=/home/michael/.local/share/OpenStudioLandscapes/.landscapes/OpenStudioLandscapes-OpenRV-Builder/OpenStudioLandscapes_OpenRV_Builder__rv_tar_xz_build_stage/tar/runs/3dc15662-ae23-43df-952d-3c0dd964a3b2:/tarballs:rw --name=OpenStudioLandscapes-ASWF-OpenRV-BuildBox-CY2024-rv_tar_xz openstudiolandscapes_openrv_builder_rv_install_clean_build_stage:2026-07-08_09-36-52__political-confusion-marred-motorist /bin/bash -c "source /etc/os-release && tar -C /rv/bin --create --verbose --owner=0 --group=0 --mode='og-w' --file - . | xz --verbose --threads=0 -9 --stdout - > /tarballs/OpenRV-4.0.0-51a2eb21-CY2024-release-Rocky-Linux-9-x86_64.tar.xz && chown ${HOST_UID}:${HOST_GID} /tarballs/OpenRV-4.0.0-51a2eb21-CY2024-release-Rocky-Linux-9-x86_64.tar.xz"

    log_records: List[str] = submit_cmds(
        context=context,
        cmds=cmds,
    )

    yield Output(tar_archive_name)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.text(tar_archive_name),
            "tar": MetadataValue.path(host_mount.joinpath(tar_archive_name)),
            "cmds": MetadataValue.md(f"```json\n{json.dumps(cmds, default=str, indent=2)}\n```"),
            "docker_run_cmd": MetadataValue.path(docker_run_cmd_str),
            "container_cmds": MetadataValue.path(container_cmds_str),
            "log_records": MetadataValue.json(log_records),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_BUILD,
    ins={
        "rv_install_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_install_build_stage"])
        ),
    },
    # retry_policy=build_docker_image_retry_policy,
)
def build_name(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    vfx_reference_platform_resource: VFXReferencePlatformResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    rv_install_build_stage: Dict,  # pylint: disable=redefined-outer-name
) -> Generator[Output[str] | Any, None, None]:
    """ """

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=rv_install_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    if auto_builder_resource.append_builder_commit_hash:
        # get commit of OpenRV-Builder
        openrv_builder_commit: str = subprocess.run(
            "git rev-parse --short HEAD",
            shell=True,
            stdout=subprocess.PIPE,
            # NameError: name '__file__' is not defined. Did you mean: '__name__'?
            cwd=pathlib.Path(__file__).parent,
        ).stdout.decode("utf-8").rstrip()

        if bool(openrv_builder_commit):
            # if not in a Git repo, an empty str is returned
            openrv_builder_commit = f"__{openrv_builder_commit}"
    else:
        openrv_builder_commit = ""

    # Todo
    #  - [ ] Maybe get a dict of the elements instead of a rigid str?
    build_name_str: str = f"OpenRV-$({openrvbuilder_resource.RV_INST_DIR}/bin/rv -version)-$(git rev-parse --short HEAD)-{vfx_reference_platform_resource.get_component.CY}-{auto_builder_resource.build_type.lower()}-${{ROCKY_SUPPORT_PRODUCT}}-$(uname --hardware-platform){openrv_builder_commit}"

    exclude_from_quote.extend(
        [
            build_name_str,
        ]
    )

    cmds = []

    docker_run_cmd = [
        shutil.which("docker"),
        # "--debug",
        f"--config={docker_config_resource.docker_config_json_root.as_posix()}",
        "run",
        "--shm-size=32g",
        "--rm",
        # "--user=$(id -u):$(id -g)",
        "--interactive",
        "--tty",
        # f"--volume={host_mount.as_posix()}:{tar_root.as_posix()}:rw",
        f"--name=OpenStudioLandscapes-ASWF-OpenRV-BuildBox-{vfx_reference_platform_resource.get_component.CY}-get-build-name",
        f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    ]

    docker_run_cmd_str = " ".join(shlex.quote(s) if not s in exclude_from_quote else s for s in docker_run_cmd)

    cmds.extend(docker_run_cmd)

    container_cmds = []

    build_name_cmd = [
        "source",
        "/etc/os-release",
        "&&",
        "echo",
        build_name_str,
    ]

    container_cmds.extend(build_name_cmd)

    container_cmds_str = " ".join(shlex.quote(s) if not s in exclude_from_quote else s for s in container_cmds)

    # exclude_from_quote.extend(
    #     [
    #         container_cmds_str,
    #     ]
    # )

    cmds.extend(
        [
            "/bin/bash",
            "-c",
            container_cmds_str
        ]
    )

    ret = subprocess.run(
        cmds,
        stdout=subprocess.PIPE,
    ).stdout.decode("utf-8").strip()

    context.log.debug(f"{ret = }")

    yield Output(ret)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(ret),
            "cmds": MetadataValue.path(" ".join(shlex.quote(s) if not s in exclude_from_quote else s for s in cmds)),
            "docker_run_cmd": MetadataValue.path(docker_run_cmd_str),
            "container_cmds": MetadataValue.path(container_cmds_str),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_APPTAINER,
    ins={
        "rv_install_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_install_clean_build_stage"])
        ),
        "build_name": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "build_name"])
        ),
    },
)
def write_apptainer_def(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    apptainer_resource: ApptainerResource,
    openrvbuilder_resource: OpenRVBuilderResource,
    rv_install_build_stage: Dict,
    build_name: str,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    apptainer_def = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "apptainer",
        "OpenRV.def",
    )

    shutil.rmtree(apptainer_def.parent, ignore_errors=True)

    apptainer_def.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=rv_install_build_stage,
        docker_config=docker_config_resource,
        env={},
    )

    #################################################

    # @formatter:off
    apptainer_def_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        BootStrap: {agent}
        From: {parent_image}

        # Tested on:
        # - [x] Manjaro
        #   - [x] Functional (without `--nv`)
        #   - [x] Functional (with `--nv`)
        # - [ ] Ubuntu 22.04
        #   - [ ] Functional
        # - [ ] Rocky9
        #   - [ ] Functional
        
        %environment
        
            export PATH="{rv_inst_dir}/bin:${{PATH}}"

        %post

            # Known Issues:
            # ERROR:******* NV-GLX Extension Missing ***********
            #     If you're using an Nvidia card, please install
            #     the optimized NVIDIA binary driver.
            #     If you're using an ATI card, please be aware 
            #     that RV has not been tested with ATI cards.
            # **************************************************
            
            dnf autoremove -y
            dnf clean all

        %labels
            Author "OpenStudioLandscapes-OpenRV-Builder"
            Framework "OpenStudioLandscapes"
            Maintainer "Michael Mussato"
            Contributors "Michael Mussato, Jean First"
            Version "v0.0.1"

        %help
            # Run
            apptainer exec --nv --bind /run/user/$UID {sif} rv
        """).format(
        sif=f"{build_name}.sif",
        rv_inst_dir=openrvbuilder_resource.RV_INST_DIR,
        agent=apptainer_resource.agent,
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        # Todo: this won't work as expected if len(tags) > 1
        parent_image=f"{build_base_parent_image_prefix}{build_base_parent_image_name}:{build_base_parent_image_tags[0]}",
    )
    # @formatter:on

    with open(apptainer_def, mode="w", encoding="utf-8") as fw:
        fw.write(apptainer_def_str)

    with open(apptainer_def, mode="r") as fr:
        apptainer_def_content = fr.read()

    yield Output(apptainer_def)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(apptainer_def),
            apptainer_def.name: MetadataValue.md(f"```shell\n{apptainer_def_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "openrvbuilder_resource": MetadataValue.md(f"```json\n{openrvbuilder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_APPTAINER,
    ins={},
)
def write_apptainer_conf(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    apptainer_conf = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "apptainer",
        "apptainer.conf",
    )

    shutil.rmtree(apptainer_conf.parent, ignore_errors=True)

    apptainer_conf.parent.mkdir(parents=True, exist_ok=True)

    # @formatter:off
    apptainer_conf_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}

        # APPTAINER.CONF
        # This is the global configuration file for Apptainer. This file controls
        # what the container is allowed to do on a particular host, and as a result
        # this file must be owned by root.
        
        # ALLOW SETUID: [BOOL]
        # DEFAULT: yes
        # Should we allow users to utilize the setuid program flow within Apptainer?
        # note1: This is the default mode, and to utilize all features, this option
        # must be enabled.  For example, without this option loop mounts of image
        # files will not work; only sandbox image directories, which do not need loop
        # mounts, will work (subject to note 2).
        # note2: If this option is disabled, it will rely on unprivileged user
        # namespaces which have not been integrated equally between different Linux
        # distributions.
        allow setuid = yes
        
        # MAX LOOP DEVICES: [INT]
        # DEFAULT: 256
        # Set the maximum number of loop devices that Apptainer should ever attempt
        # to utilize.
        max loop devices = 256
        
        # ALLOW IPC NS: [BOOL]
        # DEFAULT: yes
        # Should we allow users to request the IPC namespace?
        allow ipc ns = yes
        
        # ALLOW PID NS: [BOOL]
        # DEFAULT: yes
        # Should we allow users to request the PID namespace? Note that for some HPC
        # resources, the PID namespace may confuse the resource manager and break how
        # some MPI implementations utilize shared memory. (note, on some older
        # systems, the PID namespace is always used)
        allow pid ns = yes
        
        # ALLOW USER NS: [BOOL]
        # DEFAULT: yes
        # Should we allow users to request the USER namespace?
        allow user ns = yes
        
        # ALLOW UTS NS: [BOOL]
        # DEFAULT: yes
        # Should we allow users to request the UTS namespace?
        allow uts ns = yes
        
        # CONFIG PASSWD: [BOOL]
        # DEFAULT: yes
        # If /etc/passwd exists within the container, this will automatically append
        # an entry for the calling user.
        config passwd = yes
        
        # CONFIG GROUP: [BOOL]
        # DEFAULT: yes
        # If /etc/group exists within the container, this will automatically append
        # group entries for the calling user.
        config group = yes
        
        # CONFIG RESOLV_CONF: [BOOL]
        # DEFAULT: yes
        # If there is a bind point within the container, use the host's
        # /etc/resolv.conf.
        config resolv_conf = yes
        
        # MOUNT PROC: [BOOL]
        # DEFAULT: yes
        # Should we automatically bind mount /proc within the container?
        mount proc = yes
        
        # MOUNT SYS: [BOOL]
        # DEFAULT: yes
        # Should we automatically bind mount /sys within the container?
        mount sys = yes
        
        # MOUNT DEV: [yes/no/minimal]
        # DEFAULT: yes
        # Should we automatically bind mount /dev within the container? If 'minimal'
        # is chosen, then only 'null', 'zero', 'random', 'urandom', and 'shm' will
        # be included (the same effect as the --contain options)
        mount dev = yes
        
        # MOUNT DEVPTS: [BOOL]
        # DEFAULT: yes
        # Should we mount a new instance of devpts if there is a 'minimal'
        # /dev, or -C is passed?  Note, this requires that your kernel was
        # configured with CONFIG_DEVPTS_MULTIPLE_INSTANCES=y, or that you're
        # running kernel 4.7 or newer.
        mount devpts = yes
        
        # MOUNT HOME: [BOOL]
        # DEFAULT: yes
        # Should we automatically determine the calling user's home directory and
        # attempt to mount it's base path into the container? If the --contain option
        # is used, the home directory will be created within the session directory or
        # can be overridden with the APPTAINER_HOME or APPTAINER_WORKDIR
        # environment variables (or their corresponding command line options).
        mount home = yes
        
        # MOUNT TMP: [BOOL]
        # DEFAULT: yes
        # Should we automatically bind mount /tmp and /var/tmp into the container? If
        # the --contain option is used, both tmp locations will be created in the
        # session directory or can be specified via the  APPTAINER_WORKDIR
        # environment variable (or the --workingdir command line option).
        mount tmp = yes
        
        # MOUNT HOSTFS: [BOOL]
        # DEFAULT: no
        # Probe for all mounted file systems that are mounted on the host, and bind
        # those into the container?
        mount hostfs = no
        
        # BIND PATH: [STRING]
        # DEFAULT: Undefined
        # Define a list of files/directories that should be made available from within
        # the container. The file or directory must exist within the container on
        # which to attach to. you can specify a different source and destination
        # path (respectively) with a colon; otherwise source and dest are the same.
        # NOTE: these are ignored if apptainer is invoked with --contain except
        # for /etc/hosts and /etc/localtime. When invoked with --contain and --net,
        # /etc/hosts would contain a default generated content for localhost resolution.
        #bind path = /etc/apptainer/default-nsswitch.conf:/etc/nsswitch.conf
        #bind path = /opt
        #bind path = /scratch
        bind path = /etc/localtime
        bind path = /etc/hosts
        
        # USER BIND CONTROL: [BOOL]
        # DEFAULT: yes
        # Allow users to influence and/or define bind points at runtime? This will allow
        # users to specify bind points, scratch and tmp locations. (note: User bind
        # control is only allowed if the host also supports PR_SET_NO_NEW_PRIVS)
        user bind control = yes
        
        # ENABLE FUSEMOUNT: [BOOL]
        # DEFAULT: yes
        # Allow users to mount fuse filesystems inside containers with the --fusemount
        # command line option.
        enable fusemount = yes
        
        # ENABLE OVERLAY: [yes/no/driver/try]
        # DEFAULT: yes
        # Enabling this option will make it possible to specify bind paths to locations
        # that do not currently exist within the container.  If 'yes', kernel overlayfs
        # will be tried but if it doesn't work, the image driver (i.e. fuse-overlayfs)
        # will be used instead.  'try' is obsolete and treated the same as 'yes'.
        # If 'driver' is chosen, overlay will always be handled by the image driver.
        # If 'no' is chosen, then no overlay type will be used for missing bind paths
        # nor for any other purpose.
        # The ENABLE UNDERLAY 'preferred' option below overrides this option for
        # creating bind paths.
        enable overlay = yes
        
        # ENABLE UNDERLAY: [yes/no/preferred]
        # DEFAULT: yes
        # Enabling this option will make it possible to specify bind paths to locations
        # that do not currently exist within the container without using any overlay
        # feature, when the '--underlay' action option is given by the user or when
        # the ENABLE OVERLAY option above is set to 'no'.
        # If 'preferred' is chosen, then underlay will always be used instead of
        # overlay for creating bind paths.
        # This option is deprecated and will be removed in a future release, because
        # the implementation is complicated and the performance is similar to
        # overlayfs and fuse-overlayfs.
        enable underlay = yes
        
        # MOUNT SLAVE: [BOOL]
        # DEFAULT: yes
        # Should we automatically propagate file-system changes from the host?
        # This should be set to 'yes' when autofs mounts in the system should
        # show up in the container.
        mount slave = yes
        
        # SESSIONDIR MAXSIZE: [STRING]
        # DEFAULT: 64
        # This specifies how large the default sessiondir should be (in MB). It will
        # affect users who use the "--contain" options and don't also specify a
        # location to do default read/writes to (e.g. "--workdir" or "--home") and
        # it will also affect users of "--writable-tmpfs".
        sessiondir max size = 64
        
        # *****************************************************************************
        # WARNING
        #
        # The 'limit container' and 'allow container' directives are not effective if
        # unprivileged user namespaces are enabled. They are only effectively applied
        # when Apptainer is running using the native runtime in setuid mode, and
        # unprivileged container execution is not possible on the host.
        #
        # You must disable unprivileged user namespace creation on the host if you rely
        # on the these directives to limit container execution.
        #
        # See the 'Security' and 'Configuration Files' sections of the Admin Guide for
        # more information.
        # *****************************************************************************
        
        # LIMIT CONTAINER OWNERS: [STRING]
        # DEFAULT: NULL
        # Only allow containers to be used that are owned by a given user. If this
        # configuration is undefined (commented or set to NULL), all containers are
        # allowed to be used.
        #
        # Only effective in setuid mode, with unprivileged user namespace creation
        # disabled.  Ignored for the root user.
        #limit container owners = gmk, apptainer, nobody
        
        
        # LIMIT CONTAINER GROUPS: [STRING]
        # DEFAULT: NULL
        # Only allow containers to be used that are owned by a given group. If this
        # configuration is undefined (commented or set to NULL), all containers are
        # allowed to be used.
        #
        # Only effective in setuid mode, with unprivileged user namespace creation
        # disabled.  Ignored for the root user.
        #limit container groups = group1, apptainer, nobody
        
        
        # LIMIT CONTAINER PATHS: [STRING]
        # DEFAULT: NULL
        # Only allow containers to be used that are located within an allowed path
        # prefix. If this configuration is undefined (commented or set to NULL),
        # containers will be allowed to run from anywhere on the file system.
        #
        # Only effective in setuid mode, with unprivileged user namespace creation
        # disabled.  Ignored for the root user.
        #limit container paths = /scratch, /tmp, /global
        
        
        # ALLOW CONTAINER ${{TYPE}}: [BOOL]
        # DEFAULT: yes
        # This feature limits what kind of containers that Apptainer will allow
        # users to use.
        #
        # Only effective in setuid mode, with unprivileged user namespace creation
        # disabled.  Ignored for the root user. Note that some of the
        # same operations can be limited in setuid mode by the ALLOW SETUID-MOUNT
        # feature below; both types need to be "yes" to be allowed.
        #
        # Allow use of unencrypted SIF containers
        allow container sif = yes
        #
        # Allow use of encrypted SIF containers
        allow container encrypted = yes
        #
        # Allow use of non-SIF image formats
        allow container squashfs = yes
        allow container extfs = yes
        allow container dir = yes
        
        # ALLOW SETUID-MOUNT ${{TYPE}}: [see specific types below]
        # This feature limits what types of kernel mounts that Apptainer will
        # allow unprivileged users to use in setuid mode.  Note that some of
        # the same operations can also be limited by the ALLOW CONTAINER feature
        # above; both types need to be "yes" to be allowed.  Ignored for the root
        # user.
        #
        # ALLOW SETUID-MOUNT ENCRYPTED: [BOOL]
        # DEFAULT: yes
        # Allow mounting of SIF encryption using the kernel device-mapper in
        # setuid mode.  If set to "no", gocryptfs (FUSE-based) encryption will be
        # used instead, which uses a different format in the SIF file, the same
        # format used in unprivileged user namespace mode.
        # allow setuid-mount encrypted = yes
        #
        # ALLOW SETUID-MOUNT SQUASHFS: [yes/no/iflimited]
        # DEFAULT: iflimited
        # Allow mounting of squashfs filesystem types by the kernel in setuid mode,
        # both inside and outside of SIF files.  If set to "no", a FUSE-based
        # alternative will be used, the same one used in unprivileged user namespace
        # mode.  If set to "iflimited" (the default), then if either a LIMIT CONTAINER
        # option is used above or the Execution Control List (ECL) feature is activated
        # in ecl.toml, this setting will be treated as "yes", and otherwise it will be
        # treated as "no".
        # WARNING: in setuid mode a "yes" here while still allowing users write
        # access to the underlying filesystem data enables potential attacks on
        # the kernel.  On the other hand, a "no" here while attempting to limit
        # users to running only approved containers enables the users to potentially
        # override those limits using ptrace() functionality since the FUSE processes
        # run under the user's own uid.  So leaving this on the default setting is
        # advised.
        # allow setuid-mount squashfs = iflimited
        #
        # ALLOW SETUID-MOUNT EXTFS: [BOOL]
        # DEFAULT: no
        # Allow mounting of extfs filesystem types by the kernel in setuid mode, both
        # inside and outside of SIF files.  If set to "no", a FUSE-based alternative
        # will be used, the same one used in unprivileged user namespace mode.
        # WARNING: this filesystem type frequently has relevant kernel CVEs that take
        # a long time for vendors to patch because they are not considered to be High
        # severity since normally unprivileged users do not have write access to the
        # raw filesystem data.  That leaves the kernel vulnerable to attack when
        # this option is enabled in setuid mode. That is why this option defaults to
        # "no".  Change it at your own risk.
        # allow setuid-mount extfs = no
        
        # ALLOW NET USERS: [STRING]
        # DEFAULT: NULL
        # A list of non-root users that are permitted to use the CNI configurations
        # specified in the 'allow net networks' directive, and can join existing
        # network namespaces listed in the 'allow netns paths' directive.
        # By default only root may use CNI configurations, or join existing network
        # namespaces, except in the case of a fakeroot execution where only the
        # 40_fakeroot.conflist CNI configuration is used. The restriction only applies
        # when Apptainer is running in SUID mode and the user is non-root.
        #allow net users = gmk, apptainer
        
        
        # ALLOW NET GROUPS: [STRING]
        # DEFAULT: NULL
        # A list of non-root groups that are permitted to use the CNI configurations
        # specified in the 'allow net networks' directive, and can join existing
        # network namespaces listed in the 'allow netns paths' directive.
        # By default only root may use CNI configurations, or join existing network
        # namespaces, except in the case of a fakeroot execution where only the
        # 40_fakeroot.conflist CNI configuration is used. The restriction only applies
        # when Apptainer is running in SUID mode and the user is non-root.
        #allow net groups = group1, apptainer
        
        
        # ALLOW NET NETWORKS: [STRING]
        # DEFAULT: NULL
        # Specify the names of CNI network configurations that may be used by users and
        # groups listed in the allow net users / allow net groups directives. This restriction
        # only applies when Apptainer is running in SUID mode and the user is non-root.
        #allow net networks = bridge
        
        
        # ALLOW NETNS PATHS: [STRING]
        # DEFAULT: NULL
        # Specify the paths to network namespaces that may be joined by users and groups
        # listed in the allow net users / allow net groups directives. This restriction
        # only applies when Apptainer is running in SUID mode and the user is non-root.
        #allow netns paths = /var/run/netns/my_network
        
        
        # ALWAYS USE NV ${{TYPE}}: [BOOL]
        # DEFAULT: no
        # This feature allows an administrator to determine that every action command
        # should be executed implicitly with the --nv option (useful for GPU only
        # environments).
        always use nv = no
        
        # USE NVIDIA-NVIDIA-CONTAINER-CLI ${{TYPE}}: [BOOL]
        # DEFAULT: no
        # EXPERIMENTAL
        # If set to yes, Apptainer will attempt to use nvidia-container-cli to setup
        # GPUs within a container when the --nv flag is enabled.
        # If no (default), the legacy binding of entries in nvbliblist.conf will be performed.
        use nvidia-container-cli = no
        
        # ALWAYS USE ROCM ${{TYPE}}: [BOOL]
        # DEFAULT: no
        # This feature allows an administrator to determine that every action command
        # should be executed implicitly with the --rocm option (useful for GPU only
        # environments).
        always use rocm = no
        
        # ROOT DEFAULT CAPABILITIES: [full/file/no]
        # DEFAULT: full
        # Define default root capability set kept during runtime
        # - full: keep all capabilities (same as --keep-privs)
        # - file: keep capabilities configured for root in
        #         ${{prefix}}/etc/apptainer/capability.json
        # - no: no capabilities (same as --no-privs)
        root default capabilities = full
        
        # MEMORY FS TYPE: [tmpfs/ramfs]
        # DEFAULT: tmpfs
        # This feature allow to choose temporary filesystem type used by Apptainer.
        # Cray CLE 5 and 6 up to CLE 6.0.UP05 there is an issue (kernel panic) when Apptainer
        # use tmpfs, so on affected version it's recommended to set this value to ramfs to avoid
        # kernel panic
        memory fs type = tmpfs
        
        # CNI CONFIGURATION PATH: [STRING]
        # DEFAULT: Undefined
        # Defines path where CNI configuration files are stored
        #cni configuration path =
        
        # CNI PLUGIN PATH: [STRING]
        # DEFAULT: Undefined
        # Defines path where CNI executable plugins are stored
        #cni plugin path =
        
        
        # BINARY PATH: [STRING]
        # DEFAULT: $PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
        # Colon-separated list of directories to search for many binaries.  May include
        # "$PATH:", which will be replaced by the user's PATH when not running a binary
        # that may be run with elevated privileges from the setuid program flow.  The
        # internal bin ${{prefix}}/libexec/apptainer/bin is always included, either at the
        # beginning of "$PATH:" if it is present or at the very beginning if "$PATH:" is
        # not present.
        # binary path =
        
        # MKSQUASHFS PROCS: [UINT]
        # DEFAULT: 0 (All CPUs)
        # This allows the administrator to specify the number of CPUs for mksquashfs
        # to use when building an image.  The fewer processors the longer it takes.
        # To enable it to use all available CPU's set this to 0.
        # mksquashfs procs = 0
        mksquashfs procs = 0
        
        # MKSQUASHFS MEM: [STRING]
        # DEFAULT: Unlimited
        # This allows the administrator to set the maximum amount of memory for mkswapfs
        # to use when building an image.  e.g. 1G for 1gb or 500M for 500mb. Restricting memory
        # can have a major impact on the time it takes mksquashfs to create the image.
        # NOTE: This functionality did not exist in squashfs-tools prior to version 4.3
        # If using an earlier version you should not set this.
        # mksquashfs mem = 1G
        
        
        # SHARED LOOP DEVICES: [BOOL]
        # DEFAULT: no
        # Allow to share same images associated with loop devices to minimize loop
        # usage and optimize kernel cache (useful for MPI)
        shared loop devices = no
        
        # IMAGE DRIVER: [STRING]
        # DEFAULT: Undefined
        # This option specifies the name of an image driver provided by a plugin that
        # will be used to handle image mounts. This will override the builtin image
        # driver which provides unprivileged image mounts for squashfs, extfs,
        # overlayfs, and gocryptfs.  The overlayfs image driver will only be used
        # if the kernel overlayfs is not usable, but if the 'enable overlay' option
        # above is set to 'driver', the image driver will always be used for overlay.
        # If the driver name specified has not been registered via a plugin installation
        # the run-time will abort.
        image driver =
        
        # DOWNLOAD CONCURRENCY: [UINT]
        # DEFAULT: 3
        # This option specifies how many concurrent streams when downloading (pulling)
        # an image from cloud library.
        download concurrency = 3
        
        # DOWNLOAD PART SIZE: [UINT]
        # DEFAULT: 5242880
        # This option specifies the size of each part when concurrent downloads are
        # enabled.
        download part size = 5242880
        
        # DOWNLOAD BUFFER SIZE: [UINT]
        # DEFAULT: 32768
        # This option specifies the transfer buffer size when concurrent downloads
        # are enabled.
        download buffer size = 32768
        
        # SYSTEMD CGROUPS: [BOOL]
        # DEFAULT: yes
        # Whether to use systemd to manage container cgroups. Required for rootless cgroups
        # functionality. 'no' will manage cgroups directly via cgroupfs.
        systemd cgroups = yes
        
        # APPTHEUS SOCKET PATH: [STRING]
        # DEFAULT: /run/apptheus/gateway.sock
        # Defines apptheus socket path
        apptheus socket path = /run/apptheus/gateway.sock
        
        # ALLOW MONITORING: [BOOL]
        # DEFAULT: no
        # Allow to monitor the system resource usage of apptainer. To enable this option
        # additional tool, i.e. apptheus, is required.
        allow monitoring = no
        """).format(
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
    )
    # @formatter:on

    with open(apptainer_conf, mode="w", encoding="utf-8") as fw:
        fw.write(apptainer_conf_str)

    with open(apptainer_conf, mode="r") as fr:
        apptainer_conf_content = fr.read()

    yield Output(apptainer_conf)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(apptainer_conf),
            apptainer_conf.name: MetadataValue.md(f"```shell\n{apptainer_conf_content}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_APPTAINER,
    ins={
        "write_apptainer_def": AssetIn(
            AssetKey([*ASSET_HEADER_APPTAINER["key_prefix"], "write_apptainer_def"])
        ),
        "write_apptainer_conf": AssetIn(
            AssetKey([*ASSET_HEADER_APPTAINER["key_prefix"], "write_apptainer_conf"])
        ),
        "build_name": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "build_name"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def apptainer_sif_build(
    context: AssetExecutionContext,
    apptainer_resource: ApptainerResource,
    docker_config_resource: DockerConfigResource,
    auto_builder_resource: AutoBuilderResource,
    write_apptainer_def: pathlib.Path,  # pylint: disable=redefined-outer-name
    write_apptainer_conf: pathlib.Path,  # pylint: disable=redefined-outer-name
    build_name: str,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict[str, str | List[str] | Dict[Any, Any]]] | Any, None, None]:
    """ """

    if shutil.which("apptainer") is None:
        raise FileNotFoundError(
            "Apptainer was not found on this system. "
            "See [Requirements](https://github.com/michimussato/OpenStudioLandscapes-OpenRV-Builder/blob/main/README.md#requirements) "
            "for more information."
        )

    apptainer_sif_base = auto_builder_resource.output_base_path_as_path.joinpath(
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "apptainer",
        "sif",
        "runs",
        context.dagster_run.run_id,
    )

    cmds = []

    # sif_dst: pathlib.Path = apptainer_sif_base.joinpath(f"{build_name}__{datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}__{context.dagster_run.run_id}.sif")
    sif_dst: pathlib.Path = apptainer_sif_base.joinpath(f"{build_name}.sif")
    sif_dst.parent.mkdir(parents=True, exist_ok=True)

    env: Dict[str, pathlib.Path] = {
        "APPTAINER_TMPDIR": apptainer_sif_base.joinpath(".workdir", "tmp"),
        "APPTAINER_CACHEDIR": apptainer_sif_base.joinpath(".workdir", "cache"),
    }

    for _, v in env.items():
        v.mkdir(parents=True, exist_ok=True)

    cmd_apptainer_build = [
        *[f"{k}={v.as_posix()}" for k, v in env.items()],
        shutil.which("apptainer"),
        "--config", write_apptainer_conf.as_posix(),
    ]

    if apptainer_resource.debug:
        cmd_apptainer_build.extend(
            [
                "--debug",
            ]
        )

    cmd_apptainer_build.extend(
        [
            "build",
            "--disable-cache",  # cache should not remain on the filesystem to save space
            # https://apptainer.org/docs/user/main/build_a_container.html#alternative-compressors
            # - https://manpages.debian.org/testing/squashfs-tools/mksquashfs.1.en.html#COMPRESSORS_AVAILABLE_AND_COMPRESSOR_SPECIFIC_OPTIONS
            # Takes a lot of time, try later:
            # "--mksquashfs-args", "-comp zstd -Xcompression-level 22",
            "--force",  # overwrite an image file if it exists
            "--reproducible",  # creates a reproducible build by using the creation date of the source image
            # "--ignore-fakeroot-command",  # does not exist anymore?
            "--warn-unused-build-args",  # shows warning instead of fatal message when build args are not exact matched
            sif_dst.as_posix(),
            write_apptainer_def.as_posix()
        ]
    )

    cmds.append(
        {
            "cmd": cmd_apptainer_build,
            "env": {},
        }
    )

    context.log.info(f"{cmd_apptainer_build = }")
    context.log.info(f"{cmds = }")

    log_records: List[str] = submit_cmds(
        context=context,
        cmds=cmds,
    )

    cmd_apptainer_build_str = " ".join(shlex.quote(s) if not s in exclude_from_quote else s for s in cmd_apptainer_build)

    apptainer_default_opts = [
        "--nv",
    ]

    apptainer_default_mounts = [
        "/run/user/${UID}"
    ]

    _apptainer_default_mounts = []
    for default_mount in apptainer_default_mounts:
        _apptainer_default_mounts.extend(
            [
                "--bind",
                default_mount,
            ]
        )

    cmd_apptainer_run = [
        "QT_QPA_PLATFORM=xcb",
        shutil.which("apptainer"),
        "--config", write_apptainer_conf.as_posix(),
    ]

    if apptainer_resource.debug:
        cmd_apptainer_run.extend(
            [
                "--debug",
            ]
        )

    cmd_apptainer_run.extend(
        [
            "exec",
            # Failed to create secure directory (/run/user/1000/pulse): No such file or directory
            # WARNING: PulseAudioService: pa_context_connect() failed
            # WARNING: continuing without audio
            # EXCEPTION: No audio module available
            # "--bind", "/run/user/$(id -u)/pulse:/run/user/$(id -u)/pulse",
            # "--bind", "/run/user/${UID}/pulse",
            *_apptainer_default_mounts,
            *apptainer_default_opts,
            sif_dst.as_posix(),
            "rv",
        ]
    )

    exclude_from_quote.extend(
        apptainer_default_mounts
    )

    context.log.info(f"{cmd_apptainer_run = }")

    cmd_apptainer_run_str = " ".join(shlex.quote(s) if not s in exclude_from_quote else s for s in cmd_apptainer_run)

    yield Output(cmds)

    # docker save -o openstudiolandscapes_openrv_builder_build_stage_rv_install.tar openstudiolandscapes_openrv_builder_build_stage_rv_install:2026-06-28_14-36-05__medieval-scandalous-cheddar-course
    # apptainer build openstudiolandscapes_openrv_builder_build_stage_rv_install.sif docker-archive://openstudiolandscapes_openrv_builder_build_stage_rv_install.tar

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(cmds),
            "sif": MetadataValue.path(sif_dst),
            "cmd_apptainer_build": MetadataValue.path(cmd_apptainer_build_str),
            "cmd_apptainer_run": MetadataValue.path(cmd_apptainer_run_str),
            "log_records": MetadataValue.json(log_records),
            "bashrc_alias": MetadataValue.path(f'alias rv="{cmd_apptainer_run_str}"'),
            "zshrc_alias": MetadataValue.path(f'alias rv="{cmd_apptainer_run_str}"'),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )


@asset(
    **ASSET_HEADER_CLEANUP,
    deps=[
        AssetKey([*ASSET_HEADER_APPTAINER["key_prefix"], "apptainer_sif_build"]),
        AssetKey([*ASSET_HEADER_TARBALL["key_prefix"], "rv_tar_xz_build_stage"]),
    ],
    ins={
        "openrv_base_os_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_os_build_stage"])
        ),
        "openrv_base_rust_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_rust_build_stage"])
        ),
        "openrv_base_cmake_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_cmake_build_stage"])
        ),
        "openrv_base_ninja_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_ninja_build_stage"])
        ),
        "openrv_base_qt_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_qt_build_stage"])
        ),
        "openrv_base_pyenv_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_pyenv_build_stage"])
        ),
        "openrv_base_comp_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BASE_OS["key_prefix"], "openrv_base_comp_build_stage"])
        ),
        "rv_clone_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_clone_build_stage"])
        ),
        "rv_configure_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_configure_build_stage"])
        ),
        "rv_dependencies_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_dependencies_build_stage"])
        ),
        "rv_build_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_build_build_stage"])
        ),
        "rv_install_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_install_build_stage"])
        ),
        "rv_install_clean_build_stage": AssetIn(
            AssetKey([*ASSET_HEADER_BUILD["key_prefix"], "rv_install_clean_build_stage"])
        ),
    },
    description=textwrap.dedent(
        """
        Delete all local intermediate Docker images created during the process.
        """
    )
)
def cleanup(
    context: AssetExecutionContext,
    auto_builder_resource: AutoBuilderResource,
    docker_config_resource: DockerConfigResource,
    **kwargs,
) -> Generator[Output[List[str]] | AssetMaterialization, None, None]:
    """ """

    include_in_cleanup = [v for k, v in kwargs.items() if isinstance(v, dict)]

    context.log.debug(f"{include_in_cleanup = }")

    inputs = []

    for i in include_in_cleanup:
        inputs.append(
            f"{i['image_prefixes']}{i['image_name']}:{i['image_tags'][0]}"
        )

    context.log.debug(f"{inputs = }")

    cmd_system_df = [
        "docker",
        "system",
        "df",
        "--format",
        "json",
    ]

    cmd_image_rm = [
        "docker",
        "image",
        "rm",
        *inputs,
        "--force",
    ]

    cmd_prune = [
        "docker",
        "buildx",
        "prune",
        "--force",
    ]

    docker_system_df_before: str = subprocess.run(
        cmd_system_df,
        stdout=subprocess.PIPE,
    ).stdout.decode("utf-8").rstrip()

    context.log.info(f"{docker_system_df_before = }")

    if auto_builder_resource.enable_auto_cleanup:

        result_image_rm: str = subprocess.run(
            cmd_image_rm,
            stdout=subprocess.PIPE,
        ).stdout.decode("utf-8").rstrip()

        result_prune: str = subprocess.run(
            cmd_prune,
            stdout=subprocess.PIPE,
        ).stdout.decode("utf-8").rstrip()

        docker_system_df_after: str = subprocess.run(
            cmd_system_df,
            stdout=subprocess.PIPE,
        ).stdout.decode("utf-8").rstrip()

        context.log.info(f"{docker_system_df_after = }")

    else:
        result_image_rm: str = "Auto cleanup is disabled. No images removed."
        result_prune: str = "Auto cleanup is disabled. Cache not removed."

    yield Output(cmd_image_rm)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "cmd_image_rm": MetadataValue.path(' '.join(cmd_image_rm)),
            "result_image_rm": MetadataValue.md(f"```\n{result_image_rm}\n```"),
            "result_prune": MetadataValue.md(f"```\n{result_prune}\n```"),
            "auto_builder_resource": MetadataValue.md(f"```json\n{auto_builder_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_resource": MetadataValue.md(f"```json\n{docker_config_resource.model_dump_json(indent=2, fallback=str)}\n```"),
            "docker_config_json": MetadataValue.md(f"```json\n{docker_config_resource.docker_config_json_as_path.read_text()}\n```"),
        },
    )

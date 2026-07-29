import os
import multiprocessing
import pathlib
import textwrap
from typing import List, Union

from pydantic import (
    Field,
)

from dagster import (
    Config,
    ConfigurableResource,
)

from OpenStudioLandscapes.OpenRV_Builder.config.enums import (
    ApptainerAgents,
    DecodersFFMPEG,
    EncodersFFMPEG,
    RVBuildType,
    RVCmakeGenerator,
    CY,
    OS,
    Linux_CY2024,
    Linux_CY2025,
)


# Config vs. ConfigurableResource
#
# ConfigurableResource <-- [...] <-- Config <-- [...] <-- pydantic.BaseModel
#
# Config (aka an "Asset Config")
# - https://docs.dagster.io/api/dagster/config
# - https://docs.dagster.io/guides/operate/configuration
#
# ConfigurableResource
# - https://docs.dagster.io/api/dagster/resources
# - This class is a subclass of both :py:class:`ResourceDefinition` and :py:class:`Config`.
# - https://docs.dagster.io/guides/build/external-resources


OPENSTUDIOLANDSCAPES_CONFIGS_ROOT: pathlib.Path = pathlib.Path(
    os.environ.get("OPENSTUDIOLANDSCAPES_CONFIGS_ROOT", "~/.config/OpenStudioLandscapes")
).joinpath("OpenStudioLandscapes-OpenRV-Builder").expanduser()
OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.mkdir(parents=True, exist_ok=True)


# https://docs.dagster.io/guides/operate/configuration/using-environment-variables-and-secrets#handling-secrets
class NtfyResource(ConfigurableResource):
    ntfy_enable: bool = False
    require_auth: bool = False
    ntfy_username: str = Field(
        default="",
    )
    ntfy_password: str = Field(
        default="",
    )
    ntfy_url: str = Field(
        default="",
    )
    ntfy_topic: str = Field(
        default="",
    )
    dagster_webserver_protocol: str = "http"
    dagster_webserver_host: str = "localhost"
    dagster_webserver_port: int = 3000



config_NtfyResource_yaml: pathlib.Path = OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.joinpath("resource_ntfy.yaml")


config_DockerConfigResource_yaml: pathlib.Path = OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.joinpath("resource_docker_config.yaml")


class AutoBuilderResource(ConfigurableResource):

    # Todo
    #  - [ ] rename to enable_auto_cleanup_successful_builds
    enable_auto_cleanup: bool = Field(
        default=True,
        description=textwrap.dedent(
            """\
            Auto remove Docker images after successful OpenRV build, 
            Apptainer SIF and tarball creation.
            """
        )
    )

    auto_build_to_vfx_references: List[str] = Field(
        # type has to be str for now because I don't
        # know how to 'whitelist' the CY enum yet.
        default=[
            CY.CY2024.value,
            CY.CY2025.value,
        ],
        description=textwrap.dedent(
            """\
            Submit Run for these VFX References when new commits were detected
            by the `openrv_builder_commits_sensor`."""
        )
    )

    tz: str = Field(
        default="Europe/UTC",
    )

    build_newest_only: bool = Field(
        default=True,
        description=textwrap.dedent(
            """\
            If multiple new commits were detected, we can build
            only the latest or all previously unbuilt.
            """
        )
    )

    consider_commits_starting_from: str = Field(
        default='0c922732db5aa7ae59ff7f00e12e12f161811c79',
        description=textwrap.dedent(
            """\
            In case the sensor has never run, we want to
            specify a point in history as the starting
            point. No older commit will be considered and/or
            built automatically by the sensor
            """
        )
    )

    override_enabled_docker_cache: bool = Field(
        default=False,
        description=textwrap.dedent(
            """\
            Engine Config can set `--no-cache` to `False`.
            However, when building OpenRV, it might be useful
            to override this setting so that Docker commands
            within the OpenStudioLandscapes-OpenRV-Builder scope
            never cache.
            """,
        )
    )

    append_builder_commit_hash: bool = Field(
        default=False,
        description=textwrap.dedent(
            """\
            Append OpenStudioLandscapes-OpenRV-Builder
            commit hash to `tar` and `sif` file names.
            """
        )
    )

    output_base_path: str = Field(
        default="~/.local/share/OpenStudioLandscapes",
        description=textwrap.dedent(
            """\
            The full path to the resulting OpenStudioLandscapes-OpenRV-Builder output.
            """
        )
    )

    @property
    def output_base_path_as_path(self) -> pathlib.Path:
        return pathlib.Path(self.output_base_path).expanduser()
    
    build_type: RVBuildType = Field(
        default=RVBuildType.RELEASE,
        examples=[i.name for i in RVBuildType],
    )


config_AutoBuilderResource_yaml: pathlib.Path = OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.joinpath("resource_autobuilder.yaml")


class CloneRepoAssetConfig(Config):
    commit: str = Field(
        default="main",
        description=textwrap.dedent(
            """\
            The branch, tag or a commit to checkout.
            """
        ),
        examples=[
            "main",
            "v3.2.0",
            "0c922732db5aa7ae59ff7f00e12e12f161811c79",
        ],
    )


config_CloneRepoAssetConfig_yaml: pathlib.Path = OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.joinpath("config_clone_repo.yaml")


class ApptainerResource(ConfigurableResource):
    debug: bool = Field(
        default=False,
        description=textwrap.dedent(
            """\
            Enable `--debug` output for Apptainer `build`
            and `run` commands.
            """
        ),
    )
    agent: ApptainerAgents = Field(
        default=ApptainerAgents.DOCKER_DAEMON,
        examples=[i.name for i in ApptainerAgents],
        description=textwrap.dedent(
            """\
            Apptainer Bootstrap Agent to be used. For more information, checkout 
            [Preferred Bootstrap Agents](https://apptainer.org/docs/user/main/definition_files.html#preferred-bootstrap-agents).
            """
        ),
    )


config_ApptainerResource_yaml: pathlib.Path = OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.joinpath("resource_apptainer.yaml")


class OpenRVCodecsResource(ConfigurableResource):

    """
    For some reason we get a syntax error for
    - RV_FFMPEG_NON_FREE_DECODERS_TO_ENABLE: List[DecodersFFMPEG] = Field(
    - RV_FFMPEG_NON_FREE_ENCODERS_TO_ENABLE: List[EncodersFFMPEG] = Field(
    However, code works perfectly fine.

    Inspection Description:
    ```
    Parameters to generic types must be types
     Inspection info:
    Reports invalid usages of type hints.
    Example:
    from typing import TypeVar

    T0 = TypeVar('T1') # Argument of 'TypeVar' must be 'T0'


    def b(p: int) -> int:  # Type specified both in a comment and annotation
        # type: (int) -> int
        pass


    def c(p1, p2): # Type signature has too many arguments
        # type: (int) -> int
        pass
    Available quick-fixes offer various actions. You can rename, remove, or move problematic elements. You can also manually modify type declarations to ensure no warning is shown.

       OpenStudioLandscapes.OpenRV_Builder.config.enums
    EncodersFFMPEG: Any = enum.StrEnum(...
    ```
    """

    RV_FFMPEG_NON_FREE_DECODERS_TO_ENABLE: List[DecodersFFMPEG] = Field(
        default_factory=list,
        examples=[i.name for i in DecodersFFMPEG],
        description=textwrap.dedent("""\
            List the non-free FFMPEG EN-coders to enable here.
            For a full list, see [ffmpeg.cmake](https://github.com/AcademySoftwareFoundation/OpenRV/blob/main/cmake/dependencies/ffmpeg.cmake).
            As of now (2026-07-10), the compiled FFMPEG version is 8.0.
            """),
    )

    @property
    def rv_ffmpeg_non_free_decoders_to_enable(self) -> str:
        return ";".join(self.RV_FFMPEG_NON_FREE_DECODERS_TO_ENABLE)

    RV_FFMPEG_NON_FREE_ENCODERS_TO_ENABLE: List[EncodersFFMPEG] = Field(
        default_factory=list,
        examples=[i.name for i in EncodersFFMPEG],
        description="List the non-free FFMpeg DE-codecs to enable here. "
                    "For a full list, see [ffmpeg.cmake](https://github.com/AcademySoftwareFoundation/OpenRV/blob/main/cmake/dependencies/ffmpeg.cmake)."
    )

    @property
    def rv_ffmpeg_non_free_encoders_to_enable(self) -> str:
        return ";".join(self.RV_FFMPEG_NON_FREE_ENCODERS_TO_ENABLE)


config_OpenRVCodecsResource_yaml: pathlib.Path = OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.joinpath("resource_openrv_codecs.yaml")


class QtPackagesConfig(Config):
    # Config for `openrv_base_pyenv_build_stage`
    QT_MODULES: List[str] = Field(
        default=[
            "debug_info",
            "qt3d",
            "qt5compat",
            "qtcharts",
            "qtconnectivity",
            "qtdatavis3d",
            "qtgrpc",
            "qthttpserver",
            "qtimageformats",
            "qtlanguageserver",
            "qtlocation",
            "qtlottie",
            "qtmultimedia",
            "qtnetworkauth",
            "qtpdf",
            "qtpositioning",
            "qtquick3d",
            "qtquick3dphysics",
            "qtquickeffectmaker",
            "qtquicktimeline",
            "qtremoteobjects",
            "qtscxml",
            "qtsensors",
            "qtserialbus",
            "qtserialport",
            "qtshadertools",
            "qtspeech",
            "qtvirtualkeyboard",
            "qtwaylandcompositor",
            "qtwebchannel",
            "qtwebengine",
            "qtwebsockets",
            "qtwebview",
        ]
    )

    @property
    def qt_modules(self) -> str:
        return " ".join(self.QT_MODULES)

    QT_ARCHIVES: List[str] = Field(
        default=[
            "icu",
            "qtbase",
            "qtdeclarative",
            "qtsvg",
            "qttools",
            "qttranslations",
            "qtwayland",
        ]
    )

    @property
    def qt_archives(self) -> str:
        return " ".join(self.QT_ARCHIVES)


config_QtPackagesConfig_yaml: pathlib.Path = OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.joinpath("config_qt_packages.yaml")


class VFXReferencePlatformResource(ConfigurableResource):
    cy: CY
    os: OS

    @property
    def get_component(
            self,
    ) -> Union[
        Linux_CY2024,
        Linux_CY2025,
    ]:
        match self.cy:
            case CY.CY2024:
                match self.os:
                    case OS.LINUX:
                        return Linux_CY2024()
                    case OS.DARWIN:
                        raise NotImplementedError()
                    case OS.WINDOWS:
                        raise NotImplementedError()
            case CY.CY2025:
                match self.os:
                    case OS.LINUX:
                        return Linux_CY2025()
                    case OS.DARWIN:
                        raise NotImplementedError()
                    case OS.WINDOWS:
                        raise NotImplementedError()
            case CY.CY2026:
                match self.os:
                    case OS.LINUX:
                        raise NotImplementedError()
                    case OS.DARWIN:
                        raise NotImplementedError()
                    case OS.WINDOWS:
                        raise NotImplementedError()


config_VFXReferencePlatformResource_yaml: pathlib.Path = OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.joinpath("resource_vfx_reference_platform.yaml")


class OpenRVBuilderResource(ConfigurableResource):

    # OPTIONAL_SDK_ROOT: str = Field(
    #     default="",
    # )

    RV_DEPS_BMD_DECKLINK_SDK_ZIP_PATH: str = Field(
        default_factory=str,
        examples=['<downloads_path>/Blackmagic_DeckLink_SDK_14.1.zip'],
        description=textwrap.dedent(
            """\
            Download the Blackmagic Desktop Video SDK to add Blackmagic output
            capability to Open RV (optional):
            https://www.blackmagicdesign.com/desktopvideo_sdk.  Then set
            RV_DEPS_BMD_DECKLINK_SDK_ZIP_PATH to the path of the downloaded zip file on
            the rvcfg line.
            
            Example:
            
            rvcfg
            -DRV_DEPS_BMD_DECKLINK_SDK_ZIP_PATH='<downloads_path>/Blackmagic_DeckLink_SDK_14.1.zip'
            """
        )
    )

    RV_DEPS_APPLE_PRORES_SDK_ZIP_PATH: str = Field(
        default_factory=str,
        examples=['<downloads_path>/ProResDecoder_Linux_x86_64-15B54.zip'],
        description=textwrap.dedent(
            """\
            Contact Apple at prores@apple.com to obtain the free SDK. Then set
            RV_DEPS_APPLE_PRORES_SDK_ZIP_PATH to the path of the received zip file on
            the rvcfg line.
            
            Example:
            
            rvcfg
            -DRV_DEPS_APPLE_PRORES_SDK_ZIP_PATH='<downloads_path>/ProResDecoder_Linux_x86_64-15B54.zip'
            """
        )
    )

    NDI_SDK_ROOT: str = Field(
        # Todo
        #  - [ ] not used anywhere yet
        default_factory=str,
        examples=['<ndi_sdk_installation_root>'],
        description=textwrap.dedent(
            """\
            -- Could NOT find NDI_SDK (missing: NDI_SDK_INCLUDE_DIR NDI_SDK_LIBRARY)
            CMake Warning at src/plugins/output/NDI/CMakeLists.txt:27 (MESSAGE):
              NDI SDK not found, disabling NDI output plugin.
            
            Download the NDI SDK to add NDI output capability to Open RV (optional):
            https://ndi.video/.  Set the NDI_SDK_ROOT environment variable to the root
            of the NDI SDK installation if the NDI SDK fails to be automatically
            located.
            """
        )
    )

    # Todo
    #  - [ ] Maybe hard writing this value to a config
    #        might not be a good idea? Keep it dynamic?
    RV_BUILD_PARALLELISM: int = Field(
        default_factory=multiprocessing.cpu_count,
        description=textwrap.dedent(
            """\
            Resources:
            - [CPU Count in Python: Why os.cpu_count() Might Lie and Better Options](https://runebook.dev/en/docs/python/library/os/os.cpu_count)
            """
        )
    )

    RV_REPO: str = Field(
        default=pathlib.Path("/.rv/git/OpenRV").as_posix(),
        description=textwrap.dedent(
            """\
            The default container side repository root.
            """
        ),
    )

    RV_INST_DIR: str = Field(
        default=pathlib.Path("/rv").as_posix(),
        description=textwrap.dedent(
            """\
            The default container side OpenRV installation directory.
            """
        ),
    )

    RV_BUILD_DIR: str = Field(
        default="_build",
    )

    # Cmake Fields

    CMAKE_GENERATOR: RVCmakeGenerator = Field(
        default=RVCmakeGenerator.NINJA,
    )

    CMAKE_BASE: str = Field(
        default=pathlib.Path("/opt/cmake").as_posix(),
        description=textwrap.dedent(
            """\
            The default container side cmake installation directory.
            """
        ),
    )

    CMAKE_VERSION: str = Field(
        default="3.31.6",
    )

    # Rust Fields

    RUSTUP_HOME: str = Field(
        default=pathlib.Path("/opt/rust").as_posix(),
        description=textwrap.dedent(
            """\
            The default container side Rust installation directory.
            """
        ),
    )

    # This is the same as RUSTUP_HOME so we might
    # just merge these
    CARGO_HOME: str = Field(
        default=pathlib.Path("/opt/rust").as_posix(),
        description=textwrap.dedent(
            """\
            The default container side Cargo installation directory.
            """
        ),
    )

    # Ninja Fields

    NINJA_STATUS: str = Field(
        #default="- Ninja: [Elapsed: %w] [Edges: %f of %t (%p)] : ",
        default="- Ninja %p [%w] [%f/%t] - ",
        description=textwrap.dedent(
            """
            See https://ninja-build.org/manual.html
            """
        )
    )

    NINJA_HOME: str = Field(
        default=pathlib.Path("/opt/ninja").as_posix(),
        description=textwrap.dedent(
            """\
            The default container side Ninja installation directory.
            """
        ),
    )

    NINJA_VERSION: str = Field(
        default="1.12.1",
    )

    NINJA_FLAGS: str = Field(
        default="--verbose -k 0",
        # description=textwrap.dedent(
        #     """"""
        # )
    )

    # pyenv Fields

    PYENV_ROOT: str = Field(
        default=pathlib.Path("/opt/pyenv").as_posix(),
        description=textwrap.dedent(
            """\
            The default container side Pyenv installation directory.
            """
        ),
    )

    QT_ROOT: str = Field(
        default=pathlib.Path("/opt/Qt").as_posix(),
        description=textwrap.dedent(
            """\
            The default container side Qt installation directory.
            """
        ),
    )

    QT_GCC: str = Field(
        default="gcc_64",
    )


config_OpenRVBuilderResource_yaml: pathlib.Path = OPENSTUDIOLANDSCAPES_CONFIGS_ROOT.joinpath("resource_openrv_builder.yaml")

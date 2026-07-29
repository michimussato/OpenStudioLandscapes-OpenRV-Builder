import enum
from typing import Any


class DockerRegistryAccess(enum.StrEnum):
    public = "public"
    private = "private"


class DockerRegistryProtocol(enum.StrEnum):
    http = "http"
    https = "https"


class DockerPullPolicy(enum.StrEnum):
    never = "never"
    missing = "missing"
    always = "always"


# class CommitsTested(enum.StrEnum):
#     BRANCH_MAIN = "main"
#     COMMIT_0C922732 = "0c922732"


class PlatformLinuxGCC(enum.StrEnum):
    # These are probably tied to the OS itself
    # Are `gcc` and `cpp` the same?
    # - `cpp --version`
    # - `gcc --version`
    CY2024 = "11.2.1"
    CY2025 = "11.2.1"
    # CY2026 = "14.2"


class PlatformLinuxGLIBC(enum.StrEnum):
    CY2024 = "2.28"
    CY2025 = "2.28"
    # CY2026 = "2.28"


class PlatformCommonCppAPI(enum.StrEnum):
    CY2024 = "C++17"
    CY2025 = "C++17"
    # CY2026 = "C++20"


class PlatformCommonPython(enum.StrEnum):
    # https://www.python.org/downloads/
    CY2024 = "3.11.15"
    CY2025 = "3.11.15"
    # CY2026 = "3.13.x"


class PlatformCommonPyQt(enum.StrEnum):
    # https://pypi.org/project/PyQt6/#history
    CY2024 = "6.5.3"
    CY2025 = "6.5.3"
    # CY2026 = "6.8.3"


class PlatformCommonPySide(enum.StrEnum):
    CY2024 = "6.5.x"
    CY2025 = "6.5.x"
    # CY2026 = "6.8.x"


class PlatformCommonQt(enum.StrEnum):
    # https://wiki.qt.io/Qt_6.5_Release#Release_Plan
    CY2024 = "6.5.3"  # OpenRV: 6.5.3 (max aqt version - `aqt list-qt linux desktop`)
    CY2025 = "6.5.3"
    # CY2026 = "6.8.x"


class PlatformCommonACES(enum.StrEnum):
    CY2024 = "1.3"
    CY2025 = "2.0"
    # CY2026 = "2.0"


class PlatformCommonAlembic(enum.StrEnum):
    CY2024 = "1.8.x"
    CY2025 = "1.8.x"
    # CY2026 = "1.8.x"


class PlatformCommonBoost(enum.StrEnum):
    CY2024 = "1.82"
    CY2025 = "1.85"
    # CY2026 = "1.88"


class PlatformCommonFBX(enum.StrEnum):
    CY2024 = "2020.2 - 2020.latest"
    CY2025 = "2020.2 - 2020.latest"
    # CY2026 = "2020.2 - 2020.latest"


class PlatformCommonImath(enum.StrEnum):
    CY2024 = "3.1.x"
    CY2025 = "3.1.x"
    # CY2026 = "3.2.x"


class PlatformCommonNumPy(enum.StrEnum):
    CY2024 = "1.24.x"
    CY2025 = "1.26.x"
    # CY2026 = "2.3.x"


class PlatformCommonMKL(enum.StrEnum):
    CY2024 = "MKL 2020"
    CY2025 = "oneMKL 2024"
    # CY2026 = "oneMKL 2025"


class PlatformCommonTBB(enum.StrEnum):
    CY2024 = "TBB 2020 Update 3"
    CY2025 = "oneTBB 2021.x"
    # CY2026 = "oneTBB 2022.x"


class PlatformCommonOpenColorIO(enum.StrEnum):
    CY2024 = "2.3.x"
    CY2025 = "2.4.x"
    # CY2026 = "2.5.x"


class PlatformCommonOpenEXR(enum.StrEnum):
    CY2024 = "3.2.x"
    CY2025 = "3.3.x"
    # CY2026 = "3.4.x"


class PlatformCommonOpenSubdiv(enum.StrEnum):
    CY2024 = "3.6.x"
    CY2025 = "3.6.x"
    # CY2026 = "3.7.x"


class PlatformCommonOpenVDB(enum.StrEnum):
    CY2024 = "11.x"
    CY2025 = "12.x"
    # CY2026 = "13.x"


class PlatformCommonPtex(enum.StrEnum):
    CY2024 = "2.4.x"
    CY2025 = "2.4.x"
    # CY2026 = "2.4.x"


class OS(enum.StrEnum):
    LINUX = "Linux"
    DARWIN = "Darwin"
    WINDOWS = "Windows"


class CY(enum.StrEnum):
    CY2024 = "CY2024"
    CY2025 = "CY2025"
    CY2026 = "CY2026"


class Linux_CY2024:
    CY: CY = CY.CY2024
    OS: OS = OS.LINUX
    GCC: PlatformLinuxGCC = PlatformLinuxGCC.CY2024
    GLIBC: PlatformLinuxGLIBC = PlatformLinuxGLIBC.CY2024
    CPP_API: PlatformCommonCppAPI = PlatformCommonCppAPI.CY2024
    PYTHON: PlatformCommonPython = PlatformCommonPython.CY2024
    PYQT: PlatformCommonPyQt = PlatformCommonPyQt.CY2024
    PYSIDE: PlatformCommonPySide = PlatformCommonPySide.CY2024
    QT: PlatformCommonQt = PlatformCommonQt.CY2024
    ACES: PlatformCommonACES = PlatformCommonACES.CY2024
    ALEMBIC: PlatformCommonAlembic = PlatformCommonAlembic.CY2024
    BOOST: PlatformCommonBoost = PlatformCommonBoost.CY2024
    FBX: PlatformCommonFBX = PlatformCommonFBX.CY2024
    IMATH: PlatformCommonImath = PlatformCommonImath.CY2024
    NUMPY: PlatformCommonNumPy = PlatformCommonNumPy.CY2024
    MKL: PlatformCommonMKL = PlatformCommonMKL.CY2024
    TBB: PlatformCommonTBB = PlatformCommonTBB.CY2024
    OPENCOLORIO: PlatformCommonOpenColorIO = PlatformCommonOpenColorIO.CY2024
    OPENEXR: PlatformCommonOpenEXR = PlatformCommonOpenEXR.CY2024
    OPENSUBDIV: PlatformCommonOpenSubdiv = PlatformCommonOpenSubdiv.CY2024
    OPENVDB: PlatformCommonOpenVDB = PlatformCommonOpenVDB.CY2024
    PTEX: PlatformCommonPtex = PlatformCommonPtex.CY2024


class Linux_CY2025:
    CY: CY = CY.CY2025
    OS: OS = OS.LINUX
    GCC: PlatformLinuxGCC = PlatformLinuxGCC.CY2025
    GLIBC: PlatformLinuxGLIBC = PlatformLinuxGLIBC.CY2025
    CPP_API: PlatformCommonCppAPI = PlatformCommonCppAPI.CY2025
    PYTHON: PlatformCommonPython = PlatformCommonPython.CY2025
    PYQT: PlatformCommonPyQt = PlatformCommonPyQt.CY2025
    PYSIDE: PlatformCommonPySide = PlatformCommonPySide.CY2025
    QT: PlatformCommonQt = PlatformCommonQt.CY2025
    ACES: PlatformCommonACES = PlatformCommonACES.CY2025
    ALEMBIC: PlatformCommonAlembic = PlatformCommonAlembic.CY2025
    BOOST: PlatformCommonBoost = PlatformCommonBoost.CY2025
    FBX: PlatformCommonFBX = PlatformCommonFBX.CY2025
    IMATH: PlatformCommonImath = PlatformCommonImath.CY2025
    NUMPY: PlatformCommonNumPy = PlatformCommonNumPy.CY2025
    MKL: PlatformCommonMKL = PlatformCommonMKL.CY2025
    TBB: PlatformCommonTBB = PlatformCommonTBB.CY2025
    OPENCOLORIO: PlatformCommonOpenColorIO = PlatformCommonOpenColorIO.CY2025
    OPENEXR: PlatformCommonOpenEXR = PlatformCommonOpenEXR.CY2025
    OPENSUBDIV: PlatformCommonOpenSubdiv = PlatformCommonOpenSubdiv.CY2025
    OPENVDB: PlatformCommonOpenVDB = PlatformCommonOpenVDB.CY2025
    PTEX: PlatformCommonPtex = PlatformCommonPtex.CY2025


class RVCmakeGenerator(enum.StrEnum):
    # cmake --help
    NINJA = "Ninja"
    UNIX_MAKEFILES = "Unix Makefiles"


class RVToolChain(enum.StrEnum):
    LINUX = ""


class RVBuildType(enum.StrEnum):
    RELEASE = "Release"
    DEBUG = "Debug"


class RVBuildTarget(enum.StrEnum):
    DEPENDENCIES = "dependencies"
    MAIN_EXECUTABLE = "main_executable"


class ApptainerAgents(enum.StrEnum):
    DOCKER = "docker"
    DOCKER_DAEMON = "docker-daemon"


class Targets(enum.StrEnum):
    openrv_base_os = "openrv_base_os"
    openrv_base_rust = "openrv_base_rust"
    openrv_base_cmake = "openrv_base_cmake"
    openrv_base_ninja = "openrv_base_ninja"
    openrv_base_pyenv = "openrv_base_pyenv"
    openrv_base_qt = "openrv_base_qt"
    openrv_base_comp = "openrv_base_comp"
    rv_clone = "rv_clone"
    rv_configure = "rv_configure"
    rv_dependencies = "rv_dependencies"
    rv_build = "rv_build"
    rv_install = "rv_install"
    rv_install_clean = "rv_install_clean"
    rv_install_vanilla_rocky = "rv_install_vanilla_rocky"
    rv_tar_xz = "rv_tar_xz"
    rv_apptainer = "rv_apptainer"


class _DecodersFFMPEG(enum.StrEnum):
    AAC = "aac"
    AAC_AT = "aac_at"
    HEVC = "hevc"
    AAC_FIXED = "aac_fixed"
    AAC_LATM = "aac_latm"
    AC3 = "ac3"
    BINK = "bink"
    BINKAUDIO_DCT = "binkaudio_dct"
    BINKAUDIO_RDFT = "binkaudio_rdft"
    DNXHD = "dnxhd"
    DVVIDEO = "dvvideo"
    PRORES = "prores"
    QTRLE = "qtrle"
    VP9 = "vp9"
    VP9_CUVID = "vp9_cuvid"
    VP9_MEDIACODEC = "vp9_mediacodec"
    VP9_QSV = "vp9_qsv"
    VP9_RKMPP = "vp9_rkmpp"
    VP9_V4L2M2 = "vp9_v4l2m2m"


DecodersFFMPEG: Any = enum.StrEnum(
    # A wrapper Enum so that we can
    # define a dynamic ALL member
    "DecodersFFMPEG",
    {
        **{i.name: i.value for i in _DecodersFFMPEG},
        "ALL": ";".join([i.value for i in _DecodersFFMPEG])
    }
)


class _EncodersFFMPEG(enum.StrEnum):
    AAC = "aac"
    AAC_MF = "aac_mf"
    DNXHD = "dnxhd"
    DVVIDEO = "dvvideo"
    PRORES = "prores"
    QTRLE = "qtrle"
    VP9_QSV = "vp9_qsv"
    VP9_VAAPI = "vp9_vaapi"


EncodersFFMPEG: Any = enum.StrEnum(
    # A wrapper Enum so that we can
    # define a dynamic ALL member
    "EncodersFFMPEG",
    {
        **{i.name: i.value for i in _EncodersFFMPEG},
        "ALL": ";".join([i.value for i in _EncodersFFMPEG])
    }
)

{ pkgs, lib, config, inputs, ... }:

# Templates
# - https://github.com/myrheimb/devenv-python-uv
# - https://gitlab.com/devenv-templates/python

{
  # https://devenv.sh/basics/
  # Default Configs location where the an
  # OpenStudioLandscapes-OpenRV-Builder
  # subdirectory will be created:
  # - ~/.config/OpenStudioLandscapes
  env = {
    DAGSTER_HOME = "${config.devenv.root}/.dagster_home";
    DAGSTER_WORKSPACE = "${config.devenv.root}/.dagster_home/workspace.yaml";
    DOCKER_CONFIG_JSON = "${config.devenv.root}/.docker/config.json";
    OPENSTUDIOLANDSCAPES_CONFIGS_ROOT = "${config.devenv.root}/.config/OpenStudioLandscapes";
    # Sensor Status can be one of:
    # - STOPPED
    # - RUNNING
    OPENSTUDIOLANDSCPES_OPENRV_BUILDER__NTFY_SENSOR_STATUS = "RUNNING";
    OPENSTUDIOLANDSCPES_OPENRV_BUILDER__CHECK_FOR_NEW_COMMITS_SENSOR_STATUS = "STOPPED";
    # Sensor interval in hours:
    OPENSTUDIOLANDSCPES_OPENRV_BUILDER__CHECK_FOR_NEW_COMMITS_SENSOR_INTERVAL = "48";
  };

  # https://devenv.sh/packages/
  packages = [
    pkgs.git
    pkgs.docker
    pkgs.apptainer
  ];

  languages.python = {
    enable = true;
    venv.enable = true;
    uv = {
      enable = true;
      sync = {
        enable = true;
        allGroups = true;             # Install all dependency groups
        # groups = [ "dev" ];  # Or pick specific ones
        # extras = [ "plotting" ];    # Specific extras
        allExtras = true;           # All extras
      };
    };
    version = "3.11";
  };

  # https://devenv.sh/languages/
  # languages.rust.enable = true;

  # https://devenv.sh/processes/
  # processes.dev.exec = "${lib.getExe pkgs.watchexec} -n -- ls -la";
  processes = {
    openstudiolandscapes-openrv-builder = {
      exec = "dagster dev --workspace $DAGSTER_WORKSPACE";
      # env = {
      #   OPENSTUDIOLANDSCAPES__CONFIGSTORE_ROOT = "${config.devenv.root}/.config/OpenStudioLandscapes/config-store";
#     #    OPENSTUDIOLANDSCAPES__CONFIGSTORE_ROOT = "${config.devenv.root}/.config/OpenStudioLandscapes";
      # };
    };
    # ping.exec = "ping localhost";
    # server = {
    #   exec = "python -m http.server";
    #   cwd = "./public";
    # };
  };

  # Files
  # - https://devenv.sh/creating-files/
  #
  # Docker Config File
  # Todo:
  #  - [ ] This was just a proof of concept
  #  - [ ] Find a better, clearer implementation
  #        the path to the Docker config.json is
  #        set in `.config/OpenStudioLandscapes/OpenStudioLandscapes-OpenRV-Builder/resource_docker_config.yaml`
  files."config.json".json = {};

  # dagster.yaml
  files.".dagster_home/dagster.yaml".yaml = {
    auto_materialize = {
      enabled = true;
      use_sensors = true;
    };
    concurrency = {
      default_op_concurrency_limit = 1;
    };
    run_queue = {
      block_op_concurrency_limited_runs = {
        enabled = true;
      };
      max_concurrent_runs = 1;
    };
    telemetry = {
      enabled = false;
    };
  };

  # workspace.yaml
  files.".dagster_home/workspace.yaml".yaml = {
    load_from = [
      {
        python_module = {
          module_name = "OpenStudioLandscapes.OpenRV_Builder._definitions_with_upstream_specs";
          location_name = "OpenStudioLandscapes-OpenRV-Builder";
        };
      }
    ];
  };

  # https://devenv.sh/services/
#  services.postgres = {
#    enable = true;
#    port = 5432;
#  };
  # Todo:
  #  - [ ] Enable PostgreSQL as backend for Dagster
  #  - [ ] Is Docker as a service needed?
  #        - pkgs.docker
  #        - How would we add $USER to the `docker` group?

  # https://devenv.sh/scripts/

  scripts.openstudiolandscapes-openrv-builder = {
    exec = ''
      dagster dev --workspace $DAGSTER_WORKSPACE
    '';
  };

  scripts.create_materializations_directory.exec = ''
    # create DAGSTER_HOME
    mkdir -p $DAGSTER_HOME
    mkdir -p $OPENSTUDIOLANDSCAPES_CONFIGS_ROOT

    # copy dagster.yaml template to DAGSTER_HOME
    # if [ ! -f $DAGSTER_HOME/dagster.yaml ]
    # then
    #   cp $DEVENV_ROOT/dagster.yaml.template $DAGSTER_HOME/dagster.yaml
    # fi
  '';

  # https://devenv.sh/basics/
  enterShell = ''
    create_materializations_directory
  '';

  # See full reference at https://devenv.sh/reference/options/
}

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
#    pkgs.libpq
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
    # default process
    # `devenv up openstudiolandscapes-openrv-builder`
#    openstudiolandscapes-openrv-builder = {
#      ports.dagit.allocate = 3000;
#      exec = config.processes.openstudiolandscapes-openrv-builder-mysql.exec;
#    };
    # `devenv up openstudiolandscapes-openrv-builder-mysql` (same as above)
    openstudiolandscapes-openrv-builder = {
      ports.dagit.allocate = 3000;
      # allocate default port
      # ports.dagit.allocate = config.processes.openstudiolandscapes-openrv-builder.ports.dagit.value;
      exec = "dagster dev --port ${toString config.processes.openstudiolandscapes-openrv-builder.ports.dagit.value} --workspace $DAGSTER_WORKSPACE";
    };
    # `devenv up openstudiolandscapes-openrv-builder-postgresql`
#    openstudiolandscapes-openrv-builder-postgresql = {
#      # allocate default port
#      # ports.dagit.allocate = config.processes.openstudiolandscapes-openrv-builder.ports.dagit.value;
##      env = {
##        DAGSTER_HOME = "${config.devenv.root}/.dagster_home_postgres";
##        DAGSTER_WORKSPACE = "${config.devenv.root}/.dagster_home_postgres/workspace.yaml";
##      };
#      exec = "dagster dev --port ${toString config.processes.openstudiolandscapes-openrv-builder.ports.dagit.value} --workspace $DAGSTER_WORKSPACE";
#    };
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

  # .dagster_home_mysql/dagster.yaml
  files.".dagster_home_mysql/dagster.yaml" = {
    copyMode = "seed";
    yaml = {
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
  };

  # .dagster_home_mysql/workspace.yaml
  files.".dagster_home_mysql/workspace.yaml" = {
    copyMode = "seed";
    yaml = {
      load_from = [
        {
          python_module = {
            module_name = "OpenStudioLandscapes.OpenRV_Builder._definitions_with_upstream_specs";
            location_name = "OpenStudioLandscapes-OpenRV-Builder";
          };
        }
      ];
    };
  };

  # .dagster_home_mysql/dagster.yaml
  files.".dagster_home_postgres/dagster.yaml" = {
    copyMode = "seed";
    yaml = {
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
      storage = {
        postgres = {
          postgres_db = {
            username = "postgres";
            password = "mysecretpassword";
            hostname = "localhost";
            db_name = "postgres";
            port = 5432;
          };
        };
      };
    };
  };

  # .dagster_home_mysql/workspace.yaml
  files.".dagster_home_postgres/workspace.yaml" = {
    copyMode = "seed";
    yaml = {
      load_from = [
        {
          python_module = {
            module_name = "OpenStudioLandscapes.OpenRV_Builder._definitions_with_upstream_specs";
            location_name = "OpenStudioLandscapes-OpenRV-Builder";
          };
        }
      ];
    };
  };

  # https://devenv.sh/services/
  # `devenv processes up`
  services.postgres = {
    enable = false;
    package = pkgs.postgresql_17;
    initialDatabases = [{ name = "postgres"; }];
    port = 5432;
  };
  # Todo:
  #  - [ ] Enable PostgreSQL as backend for Dagster
  #  - [ ] Is Docker as a service needed?
  #        - pkgs.docker
  #        - How would we add $USER to the `docker` group?

  # # https://devenv.sh/scripts/
  # scripts.openstudiolandscapes-openrv-builder-mysql = {
  #   exec = ''
  #     dagster dev --workspace $DAGSTER_WORKSPACE_MYSQL
  #   '';
  # };
  # scripts.openstudiolandscapes-openrv-builder-postgresql = {
  #   exec = ''
  #     dagster dev --workspace $DAGSTER_WORKSPACE_POSTGRESQL
  #   '';
  #   # env = {};
  # };
  scripts.openstudiolandscapes-openrv-builder = {
#    exec = config.scripts.openstudiolandscapes-openrv-builder-mysql.exec;
    exec = ''
      export DAGSTER_HOME=/home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.dagster_home_postgres
      dagster dev --workspace /home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.dagster_home_postgres/workspace.yaml
    '';
  };

  scripts.create_openstudiolandscapes_configs_root.exec = ''
    mkdir -p $OPENSTUDIOLANDSCAPES_CONFIGS_ROOT
  '';

  # https://devenv.sh/basics/
  enterShell = ''
    create_openstudiolandscapes_configs_root
  '';

  # See full reference at https://devenv.sh/reference/options/

  profiles = {
    # `devenv --profile mysql up openstudiolandscapes-openrv-builder`
    mysql.module = {
      env = {
        DAGSTER_HOME = "${config.devenv.root}/.dagster_home_mysql";
        DAGSTER_WORKSPACE = "${config.devenv.root}/.dagster_home_mysql/workspace.yaml";
      };

    };
    # `devenv --profile postgresql up openstudiolandscapes-openrv-builder`
    # `devenv --profile postgresql processes down`
    postgresql.module = { config, pkgs, ... }: {
      packages = [ pkgs.libpq ];
      # is this referring to the services dict defined in this file?
      services.postgres = {
        enable = true;
#        package = pkgs.postgresql_17;
#        createDatabase = true;
##        # TBD: hbaConf = builtins.readFile ./my-custom/directory/to/pg_hba.conf;
##        initdbArgs = [
##          "--locale=C"
##          "--encoding=UTF8"
##        ];
##        # PGHOST
##        listen_addresses = "localhost";
##        port = 5432;
#        initialDatabases = [
#          {
#            "name" = "postgres";
#            "user" = "postgres";
#            "pass" = "mysecretpassword";
#          }
#        ];
      };
      env = {
        DAGSTER_HOME = "${config.devenv.root}/.dagster_home_postgres";
        DAGSTER_WORKSPACE = "${config.devenv.root}/.dagster_home_postgres/workspace.yaml";
        DB_HOST = config.env.PGHOST;
      };
    };
  };
}

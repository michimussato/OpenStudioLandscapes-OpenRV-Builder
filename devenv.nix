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

    DAGSTER_HOME_MS = "${config.devenv.root}/.dagster_home_mysql";
    DAGSTER_WORKSPACE_MS = "${config.devenv.root}/.dagster_home_mysql/workspace.yaml";

    DAGSTER_HOME_PG = "${config.devenv.root}/.dagster_home_postgres";
    DAGSTER_WORKSPACE_PG = "${config.devenv.root}/.dagster_home_postgres/workspace.yaml";

    POSTGRES_USER = "postgres";
    POSTGRES_PASSWORD = "mysecretpassword";
    # hostname = "miniboss.meemoo.lan";
    # hostname = "localhost";
    POSTGRES_DB = "postgres";
    # POSTGRES_PORT = 7654;
  };

  # https://devenv.sh/packages/
  packages = [
    pkgs.git
    pkgs.docker
    pkgs.apptainer
    # pkgs.gocryptfs  # This is actually a runtime dependency
    # pkgs.sudo
    # pkgs.polkit  # pkexec
    pkgs.postgresql_17
    pkgs.libpq
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
    dagster-mysql = {
      ports.dagit.allocate = 3000;
      exec = "";
    };
    dagster-postgres = {
      ports.dagit.allocate = 3000;
      exec = "";
    };
#    postgres = {
#      # ports.postgres.allocate = 6543;
#      # /nix/store/x2wqaprlzcyrj90vrildw08iii70s331-start-postgres/bin/start-postgres
#      exec = "createuser --superuser your_username";
#    };
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
  files.".docker/config.json" = {
    copyMode = "symlink";
    json = {};
  };

  # .dagster_home_mysql/dagster.yaml
  files.".dagster_home_mysql/dagster.yaml" = {
    copyMode = "seed";
    yaml = {
      auto_materialize = {
        enabled = true;
        use_sensors = false;
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

  # .dagster_home_postgres/dagster.yaml
  files.".dagster_home_postgres/dagster.yaml" = {
    copyMode = "symlink";
    yaml = {
      auto_materialize = {
        enabled = true;
        use_sensors = true;
      };
      # concurrency = {
      #   default_op_concurrency_limit = 1;
      # };
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
            # username = builtins.getEnv "USER";
            db_name = config.env.POSTGRES_DB;
            username = config.env.POSTGRES_USER;
            password = config.env.POSTGRES_PASSWORD;
            # hostname = "miniboss.meemoo.lan";
            hostname = "localhost";
            # port = toString config.processes.postgres.ports.postgres.value;
            port = 5432;
          };
        };
      };
    };
  };

  # .dagster_home_postgres/workspace.yaml
  files.".dagster_home_postgres/workspace.yaml" = {
    copyMode = "symlink";
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

  # .dagster_home_postgres/workspace.yaml
  files.".dagster_home_postgres/initial_script.sql" = {
    copyMode = "symlink";
    # initialScript = ''
    #   CREATE ROLE postgres WITH LOGIN PASSWORD 'mysecretpassword' CREATEDB;
    # '';
    #  # ALTER ROLE postgres WITH LOGIN;
    #  # CREATE SCHEMA public;
    #  # GRANT USAGE ON SCHEMA public TO postgres;
    #  # GRANT CREATE ON SCHEMA public TO postgres;
    #  # GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
    # '';
    text = ''
      CREATE ROLE postgres WITH LOGIN PASSWORD 'mysecretpassword' CREATEDB;
    '';
  };

  # https://devenv.sh/services/
  # - https://youtu.be/joIeF6uuMC0?list=PLfdfqqNJDWhcDaR3vzn1PtT2aGrl4UiSk&t=949
  # Test
  # `pg_isready --host localhost --port 5432 --username postgres`
  # `devenv processes up postgres -d`
  # `devenv processes logs postgres`
  # `devenv processes list`
  # `devenv processes down postgres`

  # Todo:
  #  - [ ] Enable PostgreSQL as backend for Dagster
  #  - [ ] Is Docker as a service needed?
  #        - pkgs.docker
  #        - How would we add $USER to the `docker` group?

  scripts.create_openstudiolandscapes_configs_root.exec = ''
    mkdir -p $OPENSTUDIOLANDSCAPES_CONFIGS_ROOT
  '';

  # https://devenv.sh/basics/
  enterShell = ''
    create_openstudiolandscapes_configs_root
  '';

  # See full reference at https://devenv.sh/reference/options/

  profiles = {

    # `devenv --profile dagster-mysql-profile up`
    dagster-mysql-profile.module = {
      processes = {
        dagster-mysql = {
          exec = lib.mkForce "dagster dev --log-level=debug --port=${toString config.processes.dagster-mysql.ports.dagit.value} --workspace=$DAGSTER_WORKSPACE";
        };
      };
      env = {
        DAGSTER_HOME = config.env.DAGSTER_HOME_MS;
        DAGSTER_WORKSPACE = config.env.DAGSTER_WORKSPACE_MS;
      };
    };

    # `devenv --profile dagster-postgres-profile up`
    dagster-postgres-profile.module = {
      processes = {
        dagster-postgres = {
          exec = lib.mkForce "dagster dev --log-level=debug --port=${toString config.processes.dagster-postgres.ports.dagit.value} --workspace=$DAGSTER_WORKSPACE";
        };
      };
      env = {
        DAGSTER_HOME = config.env.DAGSTER_HOME_PG;
        DAGSTER_WORKSPACE = config.env.DAGSTER_WORKSPACE_PG;
      };
    };

    # `devenv --profile db-postgres-profile up`
    db-postgres-profile.module = {
      services.postgres = {
        enable = true;
        createDatabase = false;  # Create a database named like current user on startup
        initialDatabases = [
          {
            "name" = config.env.POSTGRES_DB;
            "user" = config.env.POSTGRES_USER;
            "pass" = config.env.POSTGRES_PASSWORD;
            # psycopg.errors.InsufficientPrivilege: permission denied for schema public
            # sqlalchemy.exc.ProgrammingError: (psycopg.errors.InsufficientPrivilege) permission denied for schema public
    #        initialSQL = ''
    #          CREATE SCHEMA public;
    #          GRANT USAGE ON SCHEMA public TO postgres;
    #          GRANT CREATE ON SCHEMA public TO postgres;
    #          GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
    #        '';
          }
        ];
        package = pkgs.postgresql_17;
        listen_addresses = "*";
        port = 5432;
        initialScript = ''
          ALTER ROLE postgres WITH LOGIN;
          CREATE SCHEMA public;
          GRANT USAGE ON SCHEMA public TO postgres;
          GRANT CREATE ON SCHEMA public TO postgres;
          GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
        '';
      };
      env = {
        POSTGRES_USER = config.env.POSTGRES_USER;
        POSTGRES_PASSWORD = config.env.POSTGRES_PASSWORD;
        POSTGRES_DB = config.env.POSTGRES_DB;
      };
    };

    # `devenv --profile postgres-fullstack up`
    postgres-fullstack = {
      extends = [ "dagster-postgres-profile" "db-postgres-profile" ];
    };

  };
}

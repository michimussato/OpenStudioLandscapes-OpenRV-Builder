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
            username = "postgres";
            password = "mysecretpassword";
            hostname = "miniboss.meemoo.lan";
            db_name = "postgres";
            port = 2345;
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
  files.".dagster_home_postgres/pg_hba.conf" = {
    copyMode = "symlink";
    # This is from a default `pg_hba.conf`
    text = ''
      # PostgreSQL Client Authentication Configuration File
      # ===================================================
      #
      # Refer to the "Client Authentication" section in the PostgreSQL
      # documentation for a complete description of this file.  A short
      # synopsis follows.
      #
      # ----------------------
      # Authentication Records
      # ----------------------
      #
      # This file controls: which hosts are allowed to connect, how clients
      # are authenticated, which PostgreSQL user names they can use, which
      # databases they can access.  Records take one of these forms:
      #
      # local         DATABASE  USER  METHOD  [OPTIONS]
      # host          DATABASE  USER  ADDRESS  METHOD  [OPTIONS]
      # hostssl       DATABASE  USER  ADDRESS  METHOD  [OPTIONS]
      # hostnossl     DATABASE  USER  ADDRESS  METHOD  [OPTIONS]
      # hostgssenc    DATABASE  USER  ADDRESS  METHOD  [OPTIONS]
      # hostnogssenc  DATABASE  USER  ADDRESS  METHOD  [OPTIONS]
      #
      # (The uppercase items must be replaced by actual values.)
      #
      # The first field is the connection type:
      # - "local" is a Unix-domain socket
      # - "host" is a TCP/IP socket (encrypted or not)
      # - "hostssl" is a TCP/IP socket that is SSL-encrypted
      # - "hostnossl" is a TCP/IP socket that is not SSL-encrypted
      # - "hostgssenc" is a TCP/IP socket that is GSSAPI-encrypted
      # - "hostnogssenc" is a TCP/IP socket that is not GSSAPI-encrypted
      #
      # DATABASE can be "all", "sameuser", "samerole", "replication", a
      # database name, a regular expression (if it starts with a slash (/))
      # or a comma-separated list thereof.  The "all" keyword does not match
      # "replication".  Access to replication must be enabled in a separate
      # record (see example below).
      #
      # USER can be "all", a user name, a group name prefixed with "+", a
      # regular expression (if it starts with a slash (/)) or a comma-separated
      # list thereof.  In both the DATABASE and USER fields you can also write
      # a file name prefixed with "@" to include names from a separate file.
      #
      # ADDRESS specifies the set of hosts the record matches.  It can be a
      # host name, or it is made up of an IP address and a CIDR mask that is
      # an integer (between 0 and 32 (IPv4) or 128 (IPv6) inclusive) that
      # specifies the number of significant bits in the mask.  A host name
      # that starts with a dot (.) matches a suffix of the actual host name.
      # Alternatively, you can write an IP address and netmask in separate
      # columns to specify the set of hosts.  Instead of a CIDR-address, you
      # can write "samehost" to match any of the server's own IP addresses,
      # or "samenet" to match any address in any subnet that the server is
      # directly connected to.
      #
      # METHOD can be "trust", "reject", "md5", "password", "scram-sha-256",
      # "gss", "sspi", "ident", "peer", "pam", "ldap", "radius" or "cert".
      # Note that "password" sends passwords in clear text; "md5" or
      # "scram-sha-256" are preferred since they send encrypted passwords.
      #
      # OPTIONS are a set of options for the authentication in the format
      # NAME=VALUE.  The available options depend on the different
      # authentication methods -- refer to the "Client Authentication"
      # section in the documentation for a list of which options are
      # available for which authentication methods.
      #
      # Database and user names containing spaces, commas, quotes and other
      # special characters must be quoted.  Quoting one of the keywords
      # "all", "sameuser", "samerole" or "replication" makes the name lose
      # its special character, and just match a database or username with
      # that name.
      #
      # ---------------
      # Include Records
      # ---------------
      #
      # This file allows the inclusion of external files or directories holding
      # more records, using the following keywords:
      #
      # include           FILE
      # include_if_exists FILE
      # include_dir       DIRECTORY
      #
      # FILE is the file name to include, and DIR is the directory name containing
      # the file(s) to include.  Any file in a directory will be loaded if suffixed
      # with ".conf".  The files of a directory are ordered by name.
      # include_if_exists ignores missing files.  FILE and DIRECTORY can be
      # specified as a relative or an absolute path, and can be double-quoted if
      # they contain spaces.
      #
      # -------------
      # Miscellaneous
      # -------------
      #
      # This file is read on server startup and when the server receives a
      # SIGHUP signal.  If you edit the file on a running system, you have to
      # SIGHUP the server for the changes to take effect, run "pg_ctl reload",
      # or execute "SELECT pg_reload_conf()".
      #
      # ----------------------------------
      # Put your actual configuration here
      # ----------------------------------
      #
      # If you want to allow non-local connections, you need to add more
      # "host" records.  In that case you will also need to make PostgreSQL
      # listen on a non-local interface via the listen_addresses
      # configuration parameter, or via the -i or -h command line switches.

      # CAUTION: Configuring the system for local "trust" authentication
      # allows any local user to connect as any PostgreSQL user, including
      # the database superuser.  If you do not trust all your local users,
      # use another authentication method.


      # TYPE  DATABASE        USER            ADDRESS                 METHOD

      # "local" is for Unix domain socket connections only
      local   all             all                                     trust
      # IPv4 local connections:
      host    all             all             127.0.0.1/32            trust
      # IPv6 local connections:
      host    all             all             ::1/128                 trust
      # Allow replication connections from localhost, by a user with the
      # replication privilege.
      local   replication     all                                     trust
      host    replication     all             127.0.0.1/32            trust
      host    replication     all             ::1/128                 trust
    '';
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
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    createDatabase = false;
    initdbArgs = [
#      "--show"
      #    98 │ VERSION=17.11
      #    99 │ PGDATA=/home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.devenv/state/postgres
      #   100 │ share_path=/nix/store/0m6z1g5zrgngyysmgj7qsvcz0xz8l8a9-postgresql-17.11/share/postgresql
      #   101 │ PGPATH=/nix/store/0m6z1g5zrgngyysmgj7qsvcz0xz8l8a9-postgresql-17.11/bin
      #   102 │ POSTGRES_SUPERUSERNAME=michael
      #   103 │ POSTGRES_BKI=/nix/store/0m6z1g5zrgngyysmgj7qsvcz0xz8l8a9-postgresql-17.11/share/postgresql/postgres.bki
      #   104 │ POSTGRESQL_CONF_SAMPLE=/nix/store/0m6z1g5zrgngyysmgj7qsvcz0xz8l8a9-postgresql-17.11/share/postgresql/postgresql.conf.sample
      #   105 │ PG_HBA_SAMPLE=/nix/store/0m6z1g5zrgngyysmgj7qsvcz0xz8l8a9-postgresql-17.11/share/postgresql/pg_hba.conf.sample
      #   106 │ PG_IDENT_SAMPLE=/nix/store/0m6z1g5zrgngyysmgj7qsvcz0xz8l8a9-postgresql-17.11/share/postgresql/pg_ident.conf.sample
      #   107 │ cp: cannot create regular file '/home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.devenv/state/postgres/postgresql.conf':
      #       │  No such file or directory

      # defaults:
      "--encoding=UTF8"
      "--locale=C"
      # additional args:
      # - https://postgrespro.com/docs/postgresql/17/app-initdb
      "--debug"
      # "--allow-group-access"
      # "--username=postgres"
      # "--auth=trust"
      # "--auth-host=trust"
      # "--auth-local=trust"
    ];
    # hbaConf = builtins.readFile "${config.devenv.root}/.dagster_home_postgres/pg_hba.conf";
    # listen_addresses = "localhost,127.0.0.1";
    # listen_addresses = "localhost";
    # listen_addresses = "*";
    initialDatabases = [
      {
        "name" = "postgres";
        "user" = "michael";
        "pass" = "mysecretpassword";
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
    port = 5432;
    listen_addresses = "*";
    # initialScript = ''
    #   CREATE USER mydbuser WITH LOGIN PASSWORD 'mysecretpassword';
    # '';
      # ALTER ROLE postgres WITH LOGIN;
      # CREATE SCHEMA public;
      # GRANT USAGE ON SCHEMA public TO postgres;
      # GRANT CREATE ON SCHEMA public TO postgres;
      # GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
#    '';
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
  # scripts.openstudiolandscapes-openrv-builder = {
# #    exec = config.scripts.openstudiolandscapes-openrv-builder-mysql.exec;
  #   exec = ''
  #     export DAGSTER_HOME=/home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.dagster_home_postgres
  #     dagster dev --workspace /home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.dagster_home_postgres/workspace.yaml
  #   '';
  # };

  scripts.create_openstudiolandscapes_configs_root.exec = ''
    mkdir -p $OPENSTUDIOLANDSCAPES_CONFIGS_ROOT
  '';

  # https://devenv.sh/basics/
  enterShell = ''
    create_openstudiolandscapes_configs_root
  '';

  # See full reference at https://devenv.sh/reference/options/

  profiles = {
    # `devenv --profile dagster-mysql up`
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
    # `devenv --profile dagster-postgres up`
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
    # Todo
    #  - [ ] PostgreSQL does not work yet
     # `devenv --profile postgresql up openstudiolandscapes-openrv-builder`
     # `devenv --profile postgresql processes down`
#    postgresql.module = { config, pkgs, ... }: {
#      packages = [ pkgs.libpq ];
#      # is this referring to the services dict defined in this file?
#      services.postgres = {
#        enable = true;
#        hbaConf = builtins.readFile "${config.devenv.root}/.dagster_home_postgres/pg_hba.conf";
#        package = pkgs.postgresql_17;
# #       createDatabase = true;
# ##       # TBD: hbaConf = builtins.readFile ./my-custom/directory/to/pg_hba.conf;
# ##       initdbArgs = [
# ##         "--locale=C"
# ##         "--encoding=UTF8"
# ##       ];
# ##       # PGHOST
# ##       listen_addresses = "localhost";
#        port = 5432;
##        initialDatabases = [
# #         {
# #           "name" = "postgres";
# #           "user" = "postgres";
# #           "pass" = "mysecretpassword";
# #         }
# #       ];
#      };
#      env = {
#        DAGSTER_HOME = "${config.devenv.root}/.dagster_home_postgres";
#        DAGSTER_WORKSPACE = "${config.devenv.root}/.dagster_home_postgres/workspace.yaml";
#        # DB_HOST = config.env.PGHOST;
#      };
#    };
  };
}



```shell
nix-shell -p postgresql_17 bat
```

Settings based on:
```shell
ssh user@miniboss.meemoo.lan cat /data/local/git/repos/OpenStudioLandscapes/.dagster-postgres/docker-compose.yml | bat --language yaml
```

Environment Variables:
```shell
psql --help=variables
```

Exports for `miniboss.meemoo.lan`
```shell
set -a
PGHOST="miniboss.meemoo.lan"
PGPORT="2345"
PGUSER="postgres"
PGPASSWORD="mysecretpassword"
PGDATABASE="postgres"
set +a
```

Exports for `localhost`
Test: `pg_isready --host localhost --port 5432 --username postgres`
```shell
set -a
PGHOST="localhost"
PGPORT="5432"
PGUSER="michael"
PGPASSWORD="mysecretpassword"
PGDATABASE="postgres"
set +a
```

```shell
PGPASSWORD="mysecretpassword" psql --host=miniboss.meemoo.lan --port=2345 --username=postgres
# or
PGHOST=miniboss.meemoo.lan PGPORT="2345" PGDATABASE="postgres" PGUSER="postgres" PGPASSWORD="mysecretpassword" psql
```

List DBs:
```shell
psql --list
# or
psql --command "\l"
```

List Users:
```shell
psql --command "SELECT * FROM pg_user;"
#  usename  | usesysid | usecreatedb | usesuper | userepl | usebypassrls |  passwd  | valuntil | useconfig 
# ----------+----------+-------------+----------+---------+--------------+----------+----------+-----------
#  postgres |       10 | t           | t        | t       | t            | ******** |          | 
# (1 row)
```

List Roles:
```shell
psql --command "SELECT * FROM pg_roles;"
#            rolname           | rolsuper | rolinherit | rolcreaterole | rolcreatedb | rolcanlogin | rolreplication | rolconnlimit | rolpassword | rolvaliduntil | rolbypassrls | rolconfig | oid  
# -----------------------------+----------+------------+---------------+-------------+-------------+----------------+--------------+-------------+---------------+--------------+-----------+------
#  pg_database_owner           | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 6171
#  pg_read_all_data            | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 6181
#  pg_write_all_data           | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 6182
#  pg_monitor                  | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 3373
#  pg_read_all_settings        | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 3374
#  pg_read_all_stats           | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 3375
#  pg_stat_scan_tables         | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 3377
#  pg_read_server_files        | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 4569
#  pg_write_server_files       | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 4570
#  pg_execute_server_program   | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 4571
#  pg_signal_backend           | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 4200
#  pg_checkpoint               | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 4544
#  pg_maintain                 | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 6337
#  pg_use_reserved_connections | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 4550
#  pg_create_subscription      | f        | t          | f             | f           | f           | f              |           -1 | ********    |               | f            |           | 6304
#  postgres                    | t        | t          | t             | t           | t           | t              |           -1 | ********    |               | t            |           |   10
# (16 rows)
```

List Schemas:
```shell
psql --command "\dn"
# or
psql --command "SELECT schema_name FROM information_schema.schemata;"
```

---

```
$ devenv processes logs postgres
==> stdout <==

PostgreSQL database directory appears to contain a database; Skipping initialization

==> stderr <==
2026-10-31 12:13:59.780 GMT [13955] LOG:  starting PostgreSQL 17.11 on x86_64-pc-linux-gnu, compiled by clang version 21.1.8, 64-bit
2026-10-31 12:13:59.780 GMT [13955] LOG:  listening on Unix socket "/run/user/1001/devenv-e7795a4/postgres/.s.PGSQL.5432"
2026-10-31 12:13:59.785 GMT [13966] LOG:  database system was interrupted; last known up at 2026-10-31 12:08:36 GMT
2026-10-31 12:14:00.178 GMT [13966] LOG:  database system was not properly shut down; automatic recovery in progress
2026-10-31 12:14:00.180 GMT [13966] LOG:  redo starts at 0/14F3950
2026-10-31 12:14:00.181 GMT [13966] LOG:  invalid record length at 0/15391E0: expected at least 24, got 0
2026-10-31 12:14:00.181 GMT [13966] LOG:  redo done at 0/15391A8 system usage: CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.00 s
2026-10-31 12:14:00.184 GMT [13964] LOG:  checkpoint starting: end-of-recovery immediate wait
2026-10-31 12:14:00.199 GMT [13964] LOG:  checkpoint complete: wrote 42 buffers (0.3%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.004 s, sync=0.007 s, total=0.017 s; sync files=11, longest=0.002 s, average=0.001 s; distance=278 kB, estimate=278 kB; lsn=0/15391E0, redo lsn=0/15391E0
2026-10-31 12:14:00.201 GMT [13955] LOG:  database system is ready to accept connections
2026-10-31 12:14:01.770 GMT [13972] FATAL:  role "michael" does not exist
2026-10-31 12:14:01.774 GMT [13975] FATAL:  role "michael" does not exist
```

---

Dagster with `use_sensers: false`
```
  262 │ WARNING:root:Retrying failed database connection: (psycopg.OperationalError) connection failed: connection to server at "127.0.0.1", port 5432 failed: Connection refused
  263 │ WARNING:Is the server running on that host and accepting TCP/IP connections?
  264 │ Multiple connection attempts failed. All failures were:
  265 │ - host: 'localhost', port: 5432, hostaddr: '::1': connection failed: connection to server at "::1", port 5432 failed: Connection refused
  266 │ - host: Is the server running on that host and accepting TCP/IP connections?
  267 │ - host: 'localhost', port: 5432, hostaddr: '127.0.0.1': connection failed: connection to server at "127.0.0.1", port 5432 failed: Connection refused
  268 │ - host: Is the server running on that host and accepting TCP/IP connections?
  269 │ (Background on this error at: https://sqlalche.me/e/21/e3q8)
```

```shell
sudo ss -tlnp | grep postgres
# LISTEN 0      200               0.0.0.0:5433       0.0.0.0:*    users:(("postgres",pid=38052,fd=6))
# LISTEN 0      200                  [::]:5433          [::]:*    users:(("postgres",pid=38052,fd=7))
```

```shell
psql --command "SELECT instance_info.run_storage_id FROM instance_info;"
#             run_storage_id            
# --------------------------------------
#  cc4d2021-8504-4938-8750-6875ff5787d3
# (1 row)
```

```shell
DAGSTER_HOME=/home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.dagster_home_mysql dagster-webserver -w /home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.dagster_home_mysql/workspace.yaml
# OK
```

```shell
DAGSTER_HOME=/home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.dagster_home_postgres \
    dagster-webserver \
    --workspace=/home/michael/git/backup_copy/repos/OpenStudioLandscapes-OpenRV-Builder/.dagster_home_postgres/workspace.yaml \
    --log-level=debug
# Not OK
```

```shell
dagster-webserver-debug --port 3456
# OK
```

#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres --set=control_password="$CONTROL_DB_PASSWORD" --set=results_password="$RESULTS_DB_PASSWORD" <<'SQL'
CREATE ROLE control LOGIN PASSWORD :'control_password';
CREATE ROLE results LOGIN PASSWORD :'results_password';
CREATE DATABASE control OWNER control;
CREATE DATABASE results OWNER results;
REVOKE CONNECT ON DATABASE control FROM PUBLIC;
REVOKE CONNECT ON DATABASE results FROM PUBLIC;
GRANT CONNECT ON DATABASE control TO control;
GRANT CONNECT ON DATABASE results TO results;
SQL

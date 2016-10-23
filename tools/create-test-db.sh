#!/usr/bin/env bash

createdb -h localhost -E UTF-8 pgtools_test
psql pgtools_test <<EOF
    DROP ROLE IF EXISTS pgtools;
    CREATE ROLE pgtools LOGIN PASSWORD 'pgtools';
    CREATE SCHEMA AUTHORIZATION pgtools;
EOF

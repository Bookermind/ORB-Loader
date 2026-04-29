#!/bin/bash

set -e
# Start SQL Server in the background
/opt/mssql/bin/sqlservr &

# Wait until SQL Server is ready
echo "Waiting for SQL Server to start..."
until /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U $DB_USER -P "$SA_PASSWORD" -Q "select 1" > /dev/null 2>&1
do
    sleep 2
done

#echo "SQL Server is up. Running initialisation script..."
/opt/mssql-tools18/bin/sqlcmd -C -S localhost -U $DB_USER -P "$SA_PASSWORD" \
    -v DB_NAME="$DB_NAME" \
    -v ADMIN_DB_SCHEMA="$ADMIN_DB_SCHEMA" \
    -v LOOKUP_DB_SCHEMA="$LOOKUP_DB_SCHEMA" \
    -i /docker-entrypoint-initdb.d/init.sqlt

#Keep container Running
wait

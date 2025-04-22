#!/bin/bash
# Test script for PostgreSQL interface

# Configuration
HOST="localhost"
PORT="54321"
PSQL_CMD="psql -h $HOST -p $PORT"

echo "Testing PostgreSQL interface on $HOST:$PORT..."

echo -e "\n1. Creating users table..."
$PSQL_CMD -c "create table users (age int, name text);"

echo -e "\n2. Inserting test data..."
$PSQL_CMD -c "insert into users values(14, 'garry'), (20, 'ted');"

echo -e "\n3. Querying all columns..."
$PSQL_CMD -c "select name, age from users;"

echo -e "\n4. Querying age column only..."
$PSQL_CMD -c "select age from users;"

echo -e "\n5. Querying name column only..."
$PSQL_CMD -c "select name from users;"

echo -e "\nAll tests completed."


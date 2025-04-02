psql -h localhost -p 54321 -c "create table users (age int, name text);"

psql -h localhost -p 54321 -c "insert into users values(14, 'garry'), (20, 'ted');"

psql -h localhost -p 54321 -c "select name, age from users;"

psql -h localhost -p 54321 -c "select age from users;"

psql -h localhost -p 54321 -c "select name from users;"


# Trino

## Download Package

```bash
mkdir -p libs
(cd libs && curl -OL https://jdbc.postgresql.org/download/postgresql-42.3.9.jar)
# curl -OL https://jdbc.postgresql.org/download/postgresql-42.7.5.jar
# curl -OL https://github.com/xerial/sqlite-jdbc/releases/download/3.49.1.0/sqlite-jdbc-3.49.1.0.jar
```

## Docker Compose

### Configure .env

```bash
cat <<EOF >.env
# Hive
HIVE_VERSION=4.0.1
HIVE_POSTGRES_DRIVER_PATH=./libs/postgresql-42.3.9.jar

S3_ENDPOINT=
S3_AWS_ACCESS_KEY=
S3_AWS_SECRET_KEY=
EOF
```

### Run

```bash
docker compose up -d
```

```bash
docker compose exec trino trino
```

```bash
trino> show catalogs;
 Catalog 
---------
 jmx     
 memory  
 system  
 tpcds   
 tpch    
(5 rows)
Query 20250313_080341_00003_8ukqf, FINISHED, 1 node
Splits: 11 total, 11 done (100.00%)
0.17 [0 rows, 0B] [0 rows/s, 0B/s]
```

### Config Files

```bash
$ tree etc/
etc/
└── catalog
    └── local.properties
1 directory, 1 file
```

#### with DBeaver

* JDBC URL : jdbc:trino://localhost:8080/local/default
* Username : admin
* Password : `<none>` [Trino - Auth](https://github.com/trinodb/trino/discussions/14704#discussioncomment-6685408)

```sql
CREATE TABLE test ( id INT, name STRING, age INT);
INSERT INTO test VALUES (1, 'hoon.yang', 39);
SELECT * FROM test;
```

#### with Hive Server (Beeline)

```bash
docker compose -f compose-hive.yaml exec hiveserver2 beeline -u 'jdbc:hive2://hiveserver2:10000/'
```

```bash
SHOW CATALOGS; SHOW SCHEMAS FROM s3;
# CREATE SCHEMA s3.test;
# CREATE TABLE s3.default.test_2 (
#     id INT,
#     name VARCHAR,
#     age INT
# );
CREATE TABLE s3.default.test_a (
    id   VARCHAR,
    name VARCHAR,
    age  VARCHAR
) WITH (
    format = 'CSV',
    external_location = 'file:///warehouse/testa/'
);
INSERT INTO s3.default.test_2 VALUES ('1', 'hoon.yang', '39');
SELECT * FROM s3.default.test_2;
```

#### with Trino CLI

```sql
CREATE TABLE test ( id INT, name STRING, age INT);
INSERT INTO test VALUES (1, 'hoon.yang', 39);
SELECT * FROM test;
```

```sql
CREATE TABLE test_1 (id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/opt/hive/data/warehouse/test_1.csv';
INSERT INTO test_1 VALUES (1, 'hoon.yang', 39);
SELECT * FROM test_1;
```

## on Podman

```bash
podman compose up

# + Install Podman
# podman machine reset
# podman machine init
# 
# podman machine start
# podman machine list
# 
# podman version

# + Install Podman and Podman Compose
# + Install Docker Compose
# podman compose up
```

## References

### Compose

* https://trino.io/docs/current/installation/containers.html
* https://medium.com/@edilsonathaydejunior/trino-hive-and-mariadb-in-minutes-with-docker-compose-7ed75f3d711c

### S3

* https://trino.io/docs/current/object-storage/file-system-s3.html

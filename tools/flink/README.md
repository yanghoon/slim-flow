### Flink

#### Usage

* Flink JobManager UI - http://localhost:8081

```bash
docker compose up -d
```

```bash
docker compose run sql-client
# Flink SQL> show catalogs;
```

#### Docker Compose File

```yaml
services:
  jobmanager:
    image: apache/flink:1.20.2
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
      - FLINK_PLUGINS_DIR=/opt/flink/plugins
    ports:
      - "8081:8081"
    volumes:
      - ./plugins:/opt/flink/plugins
      - ./conf/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:ro
    command: jobmanager

  taskmanager:
    image: apache/flink:1.20.2
    profiles:
      - task
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
      - FLINK_PLUGINS_DIR=/opt/flink/plugins
    depends_on:
      - jobmanager
    volumes:
      - ./plugins:/opt/flink/plugins
      - ./conf/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:ro
    command: taskmanager
    deploy:
      replicas: 1
```

#### Flink Config File

```yaml
# Path : conf/flink-conf.yaml
# Iceberg REST Catalog 설정 예시
iceberg.catalog-type: rest
iceberg.catalog-impl: org.apache.iceberg.rest.RESTCatalog
iceberg.uri: http://localhost:8181
iceberg.warehouse: s3://your-bucket/warehouse

# AWS S3 인증 및 endpoint (필요 시)
s3.access-key: YOUR_ACCESS_KEY
s3.secret-key: YOUR_SECRET_KEY
s3.endpoint: https://maxio.com
s3.path-style-access: true
```

#### References

- [Building a Local Flink Environment with Docker and Submitting Your First Job (tim santeford)](https://www.timsanteford.com/posts/building-a-local-flink-environment-with-docker-and-submitting-your-first-job/)  
- [Apache Flink SQL Client on Docker (DEV Community)](https://dev.to/ftisiot/apache-flink-on-docker-4kij)  
- [Setup Local Development Environment for Apache Flink and Spark Using EMR Container Images (Jaehyeon Kim)](https://jaehyeon.me/blog/2023-12-07-flink-spark-local-dev/)  

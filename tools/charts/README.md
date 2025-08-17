# Helm Charts for Compose

## Helm Repo Index

```bash
find . -maxdepth 1 -type d ! -name '.' | xargs helm package
helm repo index .
```

## Flink Compose

```bash
helm template ./flink-compose
# helm template local ./flink-compose | docker compose -f - config
```

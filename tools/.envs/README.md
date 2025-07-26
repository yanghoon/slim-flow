#

##

```bash
docker compose -f compose-environment.yaml config
```

```bash
docker compose -f compose-env-file-default.yaml config
```

```bash
docker compose -f compose-env-file-default.yaml --env-file production.env config
```

```bash
docker compose -f compose-env-file-production.yaml config
```

```bash
docker compose -f compose-env-file-production.yaml --env-file production.env config
```

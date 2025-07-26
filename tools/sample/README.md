#

##

```bash
docker compose config

VAR_OS=os docker compose config
VAR_OS=os VAR_ENVIRONMENT_HARD_CODE=os docker compose config

VAR_OS=os VAR_ENVIRONMENT_HARD_CODE=os docker compose -f compose.yaml -f compose-merge.yaml config
```

```bash
docker compose config | grep SECRET
VAR_SECRET=os docker compose config | grep SECRET
VAR_SECRET=os docker compose -f compose.yaml -f compose-merge.yaml config | grep SECRET
docker compose -f compose.yaml -f compose-merge.yaml config | grep SECRET
```

```bash
docker compose -f compose.yaml -f compose-merge.yaml --env-file production.env config | grep SECRET
docker compose -f compose.yaml -f compose-merge.yaml --env-file .env --env-file production.env config | grep SECRET
docker compose --env-file .env --env-file production.env config | grep SECRET

COMPOSE_ENV_FILES=.env,production.env docker compose config | grep SECRET
COMPOSE_ENV_FILES=.env,production.env COMPOSE_FILE="compose.yaml;compose-merge.yaml" docker compose config | grep SECRET

docker compose --env-file production+.env config | grep SECRET
COMPOSE_ENV_FILES=production+.env docker compose config | grep SECRET
```

## Local

```bash
# Setup
cp .env.temlate .env

# Execute
docker compose up
```

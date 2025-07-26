# Docker Compose Multi Environment Config

When deploy containers using Docker Compose, Each environment has some specific configurations and share many parts.
(like images, variables, ports, volumes, ...)

Repeats make hard to maintain compose files. So this project show patterns for them.

The requirements for managing Docker Compose configurations by environment are as follows:

* Run Docker Compose for each environment with minimal files and commands.
* Manage all files required for deployment with Git.
* Exclude sensitive information such as tokens and passwords from version control.

Docker Compose and environment variable files must satisfy the following rules.

1. `compose.yaml` file has a default env file. (like `default.env` file in `env_file` attribute)
2. `.env` file has variables that are secrets or only for local.
   `.env` file locates at same directory of `compose.yaml`.
   `.env` file is not used in any compose files.
3. `default.env` file has variables with default value that used for local.
4. `${environment}/.env` file has variables for specific environments.
   `${environment}/.env` file is used at `compose-reelase.yaml` and it overwrites values in `default.env` 

## Local

Create configuration file for only loacl environment (if you need), JUST run docker compose normally. 

1. Create `.env` file and write shared variables (like secrets)

```bash
cat <<EOF > .env
S3_SECRET=your-secret-value
EOF
```

2. Run Docker Compose with Local Variables

```bash
docker compose config
```

```yaml
# docker compose config
name: envs2
services:
  db:
    environment:
      APP_PROFILE: default
      APP_S3_BUCKET: maxio-bucket
      S3_BUCKET: maxio-bucket
      S3_ENDPOINT: http://maxio-remote:9000
      S3_SECRET: maxio-bucket
    image: alpine:latest
    networks:
      default: null
  web:
    environment:
      APP_PROFILE: default
      APP_S3_BUCKET: maxio-bucket
      S3_BUCKET: maxio-bucket
      S3_ENDPOINT: http://maxio-remote:9000
      S3_SECRET: maxio-bucket
    image: alpine:latest
    networks:
      default: null
networks:
  default:
    name: envs2_default
```

## Development

You need some files to patch some configuration of default(local).
After create files, you just run docker compose with a small arguments.

1. `${environment}/.env` : This file has overwirted environment values and some pre-defined compose variables.
2. `compose-release.yaml` : Thie file merged `compose.yaml` with a higher priority. it is configured by the `COMPOSE_FILE` variable in the `${environment}/.env`.

#### 1. Create `.env` file and write shared variables (like secrets)

```yaml
jobs:
  ...
    runs-on: [envs, "${{ matrix.profile }}"]
    strategy:
      matrix:
        profile: [master, worker]
    steps:
    ...
      - name: Copy Env File
        run:
        cd tools/.envs.2/

        cat <<EOF > .env
        ${{ secrets.ENV_DEV}}
        EOF

      - name: Run Docker Compose (${{ matrix.profile }})
        run:
        cd tools/.envs.2/dev/

        docker compose --profile ${{ matrix.profile }} config
```

#### 2. Run Docker Compose with Local Variables

```bash
cd tools/.envs.2/dev
docker compose --profile master config
docker compose --profile worker config
```

```yaml
# docker compose --profile master config
name: envs2
services:
  db:
    profiles:
      - master
    environment:
      APP_PROFILE: dev
      APP_S3_BUCKET: maxio-bucket
      COMPOSE_ENV_FILES: ""
      COMPOSE_FILE: ""
      RELEASE: ""
      S3_BUCKET: maxio-bucket
      S3_ENDPOINT: http://maxio-remote:9000
      S3_SECRET: maxio-bucket
    image: alpine:latest
    networks:
      default: null
  web:
    profiles:
      - master
    environment:
      APP_PROFILE: dev
      APP_S3_BUCKET: maxio-bucket
      COMPOSE_ENV_FILES: ""
      COMPOSE_FILE: ""
      RELEASE: ""
      S3_BUCKET: maxio-bucket
      S3_ENDPOINT: http://maxio-remote:9000
      S3_SECRET: maxio-bucket
    image: alpine:latest
    networks:
      default: null
networks:
  default:
    name: envs2_default
```

```yaml
# docker compose --profile worker config
name: envs2
services:
  web-worker:
    profiles:
      - worker
    environment:
      APP_PROFILE: dev
      APP_S3_BUCKET: maxio-bucket
      COMPOSE_ENV_FILES: ""
      COMPOSE_FILE: ""
      RELEASE: ""
      S3_BUCKET: maxio-bucket
      S3_ENDPOINT: http://maxio-remote:9000
      S3_SECRET: maxio-bucket
    image: alpine:latest
    networks:
      default: null
networks:
  default:
    name: envs2_default
```

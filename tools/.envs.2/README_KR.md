# 환경 별 Docker Compose 구성

환경 별 Docker Compose 구성을 관리하기 위한 요구사항은 다음과 같다.

* 최소한의 파일과 명령어로 각 환경에 맞는 Docker Compose를 실행한다.
* 배포에 필요한 모든 파일은 Git으로 관리한다.
* 단 Token, Password와 같은 민감 정보는 형상 관리 대상에서 제외한다.

Docker Compose, 환경변수 파일은 다음의 규칙을 만족한다.

1. `compose.yaml` file has a default env file. (like `default.env` file in `env_file` attribute)
2. `.env` file has variables that are secrets or only for local.
   `.env` file locates at same directory of `compose.yaml`.
   `.env` file is not used in any compose files.
3. `default.env` file has variables with default value that used for local.
4. `${environment}/.env` file has variables for specific environments.
   `${environment}/.env` file is used at `compose-reelase.yaml`

## for Local

1. Create `.env` file and write shared variables (like secrets)

```bash
cat <<EOF > .env
S3_SECRET=your-secret-value
EOF
```

2. Run Docker Compose with Local Variables

```bash
cd tools/.envs.2
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

## for Development

1. Create `.env` file and write shared variables (like secrets)

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

2. Run Docker Compose with Local Variables

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

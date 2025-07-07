# Example Environment Docker Compose

Traking environment specific configurations and running docker compose with them.

1. Basically `compose.yaml` file is load env variables from `default.env` file by `env_file` attribute.
2. `.env` within same directory of `compose.yaml` is shared at all environment.
   it is only one to configure in local and others used to load secret data from external.
3. for other environments, create directory and `.env` file for configuration
   that file is used at `compose-reelase.yaml`

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
# TODO: OUTPUT
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
# TODO: OUTPUT
```

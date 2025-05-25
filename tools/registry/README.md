# Local Docker Registry

```bash
docker compose -f compose.yaml up -d
```

## Push Images in Local

```bash
docker pull registry:3
docker tag registry:3 localhost:5000/library/registry:3
docker push --tls-verify=false localhost:5000/library/registry:3  # podman
```

## Pull Images

```bash
docker pull --tls-verify=false localhost:5000/library/registry:3  # podman
docker tag localhost:5000/library/registry:3 registry:3
```

## Appendix - Configuration

* https://distribution.github.io/distribution/storage-drivers/s3/

## Appendix - Run Registry in Air Gap

```bash
curl -OL https://raw.githubusercontent.com/ReyanL/get-object-s3/refs/heads/main/get-object-s3.sh
chmod 755 get-object-s3.sh
```

### Export

```bash
docker save registry:3 | gzip > registry_3.tgz
# Upload image archive to Object Storage
```

### Import

```bash
# export S3_HOST=localhost:9000
./get-object-s3.sh images registry_3.tgz

cksum registry_3.tgz  # 3773002923 18157621 registry_3.tgz
docker load --input registry_3.tgz
docker images | grep registry
docker compose pull registry
# [+] Pulling 7/7
#  ✔ registry Pulled
#    ✔ b5da7f963a9e Already exists
#    ✔ f18232174bc9 Already exists
#    ✔ e8a894506e86 Already exists
#    ✔ e1822bac1992 Already exists
#    ✔ e5a9c19e7b9d Already exists
#    ✔ 3dec7d02aaea Download complete
```

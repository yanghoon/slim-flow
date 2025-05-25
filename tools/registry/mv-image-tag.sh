#!/bin/bash

SOURCE_REGISTRY="${SOURCE_REGISTRY:-localhost:5000}"

IMAGES=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "${SOURCE_REGISTRY}")

for image in $IMAGES; do
  # <none> 태그나 레포는 건너뜀
  # if [[ "$image" == *"<none>:<none>"* ]]; then
  #   continue
  # fi

  repo_tag=(${image//:/ })
  # repo=${repo_tag[0]}
  # tag=${repo_tag[1]}

  # # repo나 tag가 비어있으면 건너뜀
  # if [[ -z "$repo" || -z "$tag" ]]; then
  #   continue
  # fi

  # new_image="docker.io/$DOCKERHUB_USER/$repo:$tag"

  # echo "Tagging $image as $new_image"
  # docker tag "$image" "$new_image"
  echo $repo_tag[0]
  echo $repo_tag[1]
done

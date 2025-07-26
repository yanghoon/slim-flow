#!/bin/bash

# 쉼표로 구분된 옵션을 확장해서 배열에 추가하는 함수 (파일 존재 여부 체크 X)
expand_csv_option() {
  local option="$1"
  local -n arr="$2"
  local csv="$3"
  IFS=',' read -ra values <<< "$csv"
  for v in "${values[@]}"; do
    arr+=("$option" "$v")
  done
}

env_files=()
files=()
args=()

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --env-file)
      shift
      expand_csv_option "--env-file" env_files "$1"
      shift
      ;;
    -f|--file)
      shift
      expand_csv_option "--file" files "$1"
      shift
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done

# docker compose 명령 확인
if docker compose &> /dev/null; then
  DOCKER_COMPOSE_CMD="docker compose"
elif command -v docker-compose &> /dev/null; then
  DOCKER_COMPOSE_CMD="docker-compose"
else
  echo "Error: Neither 'docker compose' nor 'docker-compose' command is available." >&2
  exit 1
fi

# 실행
echo $DOCKER_COMPOSE_CMD "${env_files[@]}" "${files[@]}" "${args[@]}"
$DOCKER_COMPOSE_CMD "${env_files[@]}" "${files[@]}" "${args[@]}"

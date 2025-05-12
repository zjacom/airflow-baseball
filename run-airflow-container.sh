#!/bin/bash

# 현재 UID / GID 확인
AIRFLOW_UID=$(id -u)
AIRFLOW_GID=$(id -g)

# .env 파일 생성 또는 업데이트
echo "AIRFLOW_UID=${AIRFLOW_UID}" > .env

# 폴더 목록
dirs=("logs" "dags" "plugins" "data")

# 각 폴더가 없다면 생성하고, 소유권 변경
for dir in "${dirs[@]}"; do
  mkdir -p "$dir"
  sudo chown -R "${AIRFLOW_UID}:${AIRFLOW_GID}" "$dir"
done

echo ".env 및 디렉토리 권한 설정 완료 ✅"

docker compose up -d

echo "Airflow 컨테이너 실행 완료 ✅"

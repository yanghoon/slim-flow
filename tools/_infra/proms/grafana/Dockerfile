FROM grafana/grafana:12.3.0-17718666199

# 플러그인 설치를 위한 환경 변수 (추가 플러그인은 COMPOSE로 설정)
ARG GF_INSTALL_PLUGINS=""

USER root

# Trino 플러그인 설치 (linux amd64)
# RUN grafana cli plugins install trino-datasource

# COMPOSE로 전달된 추가 플러그인 설치
RUN if [ ! -z "$GF_INSTALL_PLUGINS" ]; then \
      for plugin in $(echo $GF_INSTALL_PLUGINS | tr ',' ' '); do \
        grafana cli plugins install "$plugin"; \
      done; \
    fi

# 권한 복원
# RUN chown -R grafana:grafana $GF_PATHS_PLUGINS

# USER grafana

# 대시보드 프로비저닝 설정 로드 경로
# ENV GF_PATHS_PROVISIONING=/etc/grafana/provisioning

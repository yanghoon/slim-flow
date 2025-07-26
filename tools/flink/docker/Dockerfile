# https://github.com/apache/flink-docker/master/1.20/scala_2.12-jara11-ubuntu/Dockerfile
FROM flink:1.20.2

COPY script/lib.sh /opt/flink/lib.sh
RUN  chmod +x /opt/flink/lib.sh && /opt/flink/lib.sh lib
# COPY lib/*.jar  /opt/flink/lib/

ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081
CMD ["help"]
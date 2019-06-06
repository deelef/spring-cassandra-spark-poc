FROM openjdk:8-jre-slim

RUN groupadd -g 1000 appuser && \
    useradd -u 1000 -g appuser appuser
USER appuser

EXPOSE 8087
VOLUME /tmp
ARG JAR_FILE
COPY ${JAR_FILE} app.jar



ENTRYPOINT exec java -Djava.security.egd=file:/dev/./urandom $JAVA_OPTS -jar /app.jar

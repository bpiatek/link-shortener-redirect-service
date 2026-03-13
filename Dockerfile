FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

RUN groupadd -r spring && useradd -r -g spring spring
USER spring:spring

COPY --chown=spring:spring target/extracted/lib/ ./lib/

COPY --chown=spring:spring target/extracted/*.jar ./

ENTRYPOINT ["sh", "-c", "exec java -jar *.jar"]
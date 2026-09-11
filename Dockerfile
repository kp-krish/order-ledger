FROM maven:3.9.11-eclipse-temurin-21 AS build

WORKDIR /workspace
COPY pom.xml ./
RUN mvn -B -ntp dependency:go-offline

COPY src ./src
RUN mvn -B -ntp clean package -DskipTests

FROM eclipse-temurin:21.0.8_9-jre-jammy AS runtime

RUN groupadd --system --gid 10001 orderledger \
    && useradd --system --uid 10001 --gid orderledger --home-dir /app --shell /usr/sbin/nologin orderledger

WORKDIR /app
COPY --from=build --chown=orderledger:orderledger /workspace/target/order-ledger-0.0.1-SNAPSHOT.jar app.jar

USER orderledger
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

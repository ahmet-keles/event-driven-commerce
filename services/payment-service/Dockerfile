# syntax=docker/dockerfile:1
# Build the boot jar inside the image so `docker compose up --build` needs no
# host toolchain. The Maven repository is a BuildKit cache mount, so rebuilds
# only re-download dependencies when the pom changes.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY src src
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B --no-transfer-progress -DskipTests package \
    && cp target/*.jar app.jar

# Same runtime image the e2e suite runs the services on.
FROM eclipse-temurin:21-jre
RUN useradd --system --create-home appuser
USER appuser
WORKDIR /app
COPY --from=build /workspace/app.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# syntax=docker/dockerfile:1
#
# One multi-stage image for every Spring Boot app in the monorepo. Select the module with the
# MODULE build arg, e.g.:
#
#   docker build --build-arg MODULE=services/emergency-service -t ams/emergency-service .
#   docker build --build-arg MODULE=tools/uiapp                 -t ams/uiapp .
#
# Behind a TLS-intercepting proxy you can pass extra Maven flags:
#   --build-arg MAVEN_CLI_OPTS="-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.ssl.insecure=true"

# ---------- build ----------
FROM maven:3.9-eclipse-temurin-17 AS build
ARG MODULE
ARG MAVEN_CLI_OPTS=""
WORKDIR /workspace
# The reactor needs the root pom and the shared ams-schemas module, so copy the whole repo.
COPY . .
# Build only the requested module and the modules it depends on.
RUN mvn -B -ntp -DskipTests ${MAVEN_CLI_OPTS} -pl ${MODULE} -am clean package

# ---------- runtime ----------
FROM eclipse-temurin:17-jre-jammy AS runtime
ARG MODULE
WORKDIR /app
# Copy the repackaged Spring Boot jar (the *.jar.original is left behind by the glob).
COPY --from=build /workspace/${MODULE}/target/*.jar /app/app.jar
# Run as an unprivileged user.
RUN useradd --system --uid 1001 ams && chown -R ams:ams /app
USER ams
# Container-aware heap sizing; honour the k8s memory limit.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar \"$@\"", "--"]

#!/usr/bin/env bash
# Build every AMS container image from one shared Dockerfile.
# Run from anywhere; extra args are forwarded to each `docker build`, e.g.:
#   ./k8s/build-images.sh --build-arg MAVEN_CLI_OPTS="-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.ssl.insecure=true"
set -euo pipefail

cd "$(dirname "$0")/.."

apps=(
  "services/accident-event-stream:accident-event-stream"
  "services/emergency-service:emergency-service"
  "services/law-enforcement-service:law-enforcement-service"
  "services/firerescue-service:firerescue-service"
  "services/statistics-service:statistics-service"
  "services/dispatch-service:dispatch-service"
  "services/notification-service:notification-service"
  "services/incident-correlation-service:incident-correlation-service"
  "services/citizen-report-gateway:citizen-report-gateway"
  "tools/uiapp:uiapp"
  "tools/stream-bombarder-app:stream-bombarder"
)

for entry in "${apps[@]}"; do
  module="${entry%%:*}"
  name="${entry##*:}"
  echo ">> building ams/${name}:latest  (MODULE=${module})"
  docker build --build-arg MODULE="${module}" -t "ams/${name}:latest" "$@" .
done

echo "done:"
docker images "ams/*" --format "  {{.Repository}}:{{.Tag}}  {{.Size}}"

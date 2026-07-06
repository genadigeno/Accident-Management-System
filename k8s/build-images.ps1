# Build every AMS container image from one shared Dockerfile.
# Run from anywhere; extra args are forwarded to each `docker build`, e.g.:
#   .\k8s\build-images.ps1 --build-arg MAVEN_CLI_OPTS="-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.ssl.insecure=true"
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$apps = @(
  @{ module = "services/accident-event-stream";  name = "accident-event-stream" },
  @{ module = "services/emergency-service";       name = "emergency-service" },
  @{ module = "services/law-enforcement-service"; name = "law-enforcement-service" },
  @{ module = "services/firerescue-service";      name = "firerescue-service" },
  @{ module = "services/statistics-service";      name = "statistics-service" },
  @{ module = "services/dispatch-service";        name = "dispatch-service" },
  @{ module = "tools/uiapp";                      name = "uiapp" },
  @{ module = "tools/stream-bombarder-app";       name = "stream-bombarder" }
)

foreach ($a in $apps) {
  Write-Host ">> building ams/$($a.name):latest  (MODULE=$($a.module))"
  docker build --build-arg MODULE=$($a.module) -t "ams/$($a.name):latest" @args .
  if ($LASTEXITCODE -ne 0) { throw "build failed for $($a.name)" }
}

Write-Host "done:"
docker images "ams/*" --format "  {{.Repository}}:{{.Tag}}  {{.Size}}"

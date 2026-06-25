@echo off
setlocal

:: Random port so multiple instances can run on one host.
set /a RANDOM_PORT=%RANDOM% * 1000 / 32768 + 8000

set JAR_PATH=%~dp0..\services\emergency-service\target\emergency-service.jar

:: Local DB credentials (match infra/docker-compose.yml). The app reads these env vars.
set POSTGRES_USER=test
set POSTGRES_PASSWORD=postgres

echo Starting emergency-service on port %RANDOM_PORT%
java -jar "%JAR_PATH%" --server.port=%RANDOM_PORT%

endlocal

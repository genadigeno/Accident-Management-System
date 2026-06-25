@echo off
setlocal

:: Random port so multiple instances can run on one host.
set /a RANDOM_PORT=%RANDOM% * 1000 / 32768 + 8000

set JAR_PATH=%~dp0..\services\accident-event-stream\target\accident-event-stream.jar

:: Local defaults (override as needed). The app reads these env vars.
set BOOTSTRAP_SERVERS=localhost:9092,localhost:9093
set SCHEMA_REGISTRY_URL=http://localhost:8081
set SOURCE_TOPIC_NAME=accident.events
set DLT_SOURCE_TOPIC_NAME=accident.events.dlt
set STATE_DIRECTORY=.\custom-directory-%RANDOM_PORT%

echo Starting accident-event-stream on port %RANDOM_PORT%
java -jar "%JAR_PATH%" --server.port=%RANDOM_PORT%

endlocal

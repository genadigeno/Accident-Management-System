@echo off
setlocal

set JAR_PATH=%~dp0..\tools\stream-bombarder-app\target\stream-bombarder.jar

:: scale multiplies events per burst. Start small (1-10): high values flood Kafka.
set SCALE=1

echo Starting stream-bombarder (scale=%SCALE%)...
java -jar "%JAR_PATH%" --scale=%SCALE%

endlocal

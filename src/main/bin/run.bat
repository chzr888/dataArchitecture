@echo off
setLocal EnableDelayedExpansion
set CLASSPATH=%CLASSPATH%
for /R ./lib/ %%a in (*.jar) do (
set CLASSPATH=!CLASSPATH!;%%a
)
java -jar convert-data-architecture-1.0.0-SNAPSHOT.jar

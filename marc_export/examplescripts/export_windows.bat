@echo off

rem Det här skriptet kan användas som exempel på hur man automatiskt hämtar poster från Libris
rem Innan du använder det, se till att du fyllt i filen: etc/export.properties
rem Lämpligen körs detta skript minut-vis som ett schemalagt jobb. SE TILL ATT DET INTE STARTAS INNAN FÖREGÅENDE KÖRNING ÄR KLAR!

rem Om vi kör för första gången, använd 'nu' som start-tid.
IF NOT EXIST lastRun.timestamp (
        CALL :LOADUTCNOW
	@echo %currentTime%>lastRun.timestamp
)

rem Avgör tidsintervall
set /p startTime=<lastRun.timestamp
CALL :LOADUTCNOW
set stopTime=%currentTime%

rem Hämta data
curl --fail -XPOST "https://libris.kb.se/api/marc_export/?from=%startTime%&until=%stopTime%&deleted=ignore&virtualDelete=false" --data-binary @.\etc\export.properties > export.txt
if %errorlevel% neq 0 exit /b %errorlevel%

rem Om allt gick bra, uppdatera tidsstämpeln
echo %stopTime%>lastRun.timestamp

rem DINA ÄNDRINGAR HÄR, gör något produktivt med datat i 'export.txt', t ex:
rem Type export.txt




rem Slut på egna ändringar.

EXIT /B %ERRORLEVEL%

:LOADUTCNOW
for /f %%x in ('wmic path win32_utctime get /format:list ^| findstr "="') do set %%x
IF [%Month:~1,1%] == [] (
   set Month=0%Month%
)
IF [%Day:~1,1%] == [] (
   set Day=0%Day%
)
IF [%Hour:~1,1%] == [] (
   set Hour=0%Hour%
)
IF [%Minute:~1,1%] == [] (
   set Minute=0%Minute%
)
IF [%Second:~1,1%] == [] (
   set Second=0%Second%
)
set currentTime=%Year%-%Month%-%Day%T%Hour%:%Minute%:%Second%Z
EXIT /B 0

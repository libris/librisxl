# Dokumentation för Libris API för MARC21 export

Libris API för MARC21 export kan nås på:

https://libris.kb.se/api/marc_export/

För att exportera MARC data skickas en HTTP POST request till ovanstående URL med en "export-profil" som meddelande kropp samt fyra parametrar på URLen.

Dessa parametrar är:

1. "from" vilket anger början på det tidsinterval för vilket man vill ha uppdateringar. Tidsangivelsen ska vara i format ISO-8601.
2. "until" vilket anger slutet på det tidsinterval för vilket man vill ha uppdateringar. Tidsangivelsen ska vara i format ISO-8601.
3. "deleted" vilket anger hur man vill att borttagningar ska hanteras. Parametern kan va antingen värdet "ignore", vilket innebär att borttagna poster helt enkelt ignoreras, eller "export" vilket innebär att borttagna poster exporteras men är markerade som borttagna i MARC-leadern.
4. "virtualDelete" som kan ha värde "true" eller "false". Är "virtualDelete" satt till "true" så kommer poster anses vara borttagna i den genererade exporten, i dom fall där dom sigel som anges i profilen inte längre har bestånd på posterna. Flaggan används förslagsvis tillsammans med "deleted=export".

Exempel på anrop:
```
$ curl -Ss -XPOST "https://libris.kb.se/api/marc_export/?from=2019-10-05T22:00:00Z&until=2019-10-06T22:00:00Z&deleted=ignore&virtualDelete=false" --data-binary @./etc/export.properties > export.marc

```

SE UPP med era tidsangivelser/tidszoner! Exemplet ovan skickar in tider i UTC (därav 'Z' på slutet). Det är ett bra sätt att göra det på. Vill man skicka in lokala tider istället för UTC så går det också, men då måste tidszonen ingå i angivelsen. Vänligen läs på om ISO-8601!

Exempel på exportprofil:
```
move240to244=off
f003=SE-LIBR
holdupdate=on
lcsh=off
composestrategy=composelatin1
holddelete=off
authtype=interleaved
isbnhyphenate=off
name=EnBibbla
locations=S NB SM SOT SB17 NLT
bibcreate=on
authcreate=on
format=ISO2709
longname=Ett biblbiotek
extrafields=G\:050 ; Q\:050 ; L\:084,650 ; Li\:084
biblevel=off
issnhyphenate=off
issndehyphenate=off
holdtype=interleaved
holdcreate=on
characterencoding=UTF-8
isbndehyphenate=off
bibupdate=on
efilter=off
move0359=off
authupdate=on
sab=on

```

Vill man anropa detta API med ett schemalagt skript så finns exempel/förslag på sådana skript här för:
[Windows](examplescripts/export_windows.bat)
och
[*nix-derivat (bash)](examplescripts/export_nix.sh)

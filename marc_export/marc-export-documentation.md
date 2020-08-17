# Dokumentation för Libris API för MARC21 export

Libris API för MARC21 export kan nås på:

https://libris.kb.se/api/marc_export/

För att exportera MARC data skickas en HTTP POST request till ovanstående URL med en "export-profil" som meddelande kropp samt fyra parametrar på URLen.

Dessa parametrar är:

1. "from" vilket anger början på det tidsinterval för vilket man vill ha uppdateringar. Tidsangivelsen ska vara i format ISO-8601.
1. "until" vilket anger slutet på det tidsinterval för vilket man vill ha uppdateringar. Tidsangivelsen ska vara i format ISO-8601.
1. "deleted" vilket anger hur man vill att borttagningar ska hanteras. Parametern kan något utav värdena:
   1. "ignore" vilket innebär att borttagna poster helt enkelt ignoreras
   1. "export" vilket innebär att borttagna poster exporteras men är markerade som borttagna i MARC-leadern
   1. "append" vilket innebär att borttagna posters IDn exporteras som en CSV-fil _efter_ den vanliga exportdatan (separerat av en null-byte).
1. "virtualDelete" som kan ha värde "true" eller "false". Är "virtualDelete" satt till "true" så kommer poster anses vara borttagna i den genererade exporten, i dom fall där dom sigel som anges i profilen inte längre har bestånd på posterna. Flaggan används förslagsvis tillsammans med "deleted=export".

Exempel på anrop:
```
$ curl -Ss -XPOST "https://libris.kb.se/api/marc_export/?from=2019-10-05T22:00:00Z&until=2019-10-06T22:00:00Z&deleted=ignore&virtualDelete=false" --data-binary @./etc/export.properties > export.marc

```

SE UPP med era tidsangivelser/tidszoner! Exemplet ovan skickar in tider i UTC (därav 'Z' på slutet). Det är ett bra sätt att göra det på. Vill man skicka in lokala tider istället för UTC så går det också, men då måste tidszonen ingå i angivelsen. Vänligen läs på om ISO-8601!

Vill man anropa detta API med ett schemalagt skript så finns exempel/förslag på sådana skript här för:
[Windows](examplescripts/export_windows.bat)
och
[*nix-derivat (bash)](examplescripts/export_nix.sh)


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

Förklaring till (en del av) de olika inställningarna i exportprofilen:
* move240to244=off|on - Flytta MARC fältet 240 till 244
* f003=[sträng] - Tvinga fält 003 att anta ett visst värde
* nameform=Forskningsbiblioteksform - Tvinga namnformer att anta anta Forskningsbiblioteksform
* authoperators=[lista av sigel] - Ändringar av auktoritetsposter ska bara resultera i export om de gjorts av någon av följande sigel (lämnas tom för "alla")
* holdupdate=on|off - Avgör om uppdateringar av bestånd ska leda till export
* lcsh=on|off - Generera LCSH fält i posten
* composestrategy=compose|decompose - Avgör ifall unicode-tecken ska vara composed eller decomposed
* holddelete=on|off - Avgör om borttagning av bestånd ska resultera i export
* authtype=interleaved|after - Avgör om auktoritetsinformation ska vara inbakad i bib-posten eller följa med som separat därefter
* isbnhyphenate=on|off - Lägg till bindestreck i ISBN eller ej
* isbndehyphenate=on|off - Ta bort bindestreck i ISBN eller ej
* locations=[lista av sigel]|* - Generera export för dessa sigel (alternativt * för alla sigel)
* bibcreate=on|off - Avgör om nyskapande av bib post ska resultera i export
* format=ISO2709|MARCXML - Avgör serialisering av MARC-data
* holdoperators=[list av sigel] - Ändringar av beståndsposter ska bara resultera i export om de gjorts av någon av följande sigel (lämnas tom för "alla")
* authcreate=on|off - Ska skapande av auktoritetsposter kunna resultera i export
* issnhyphenate=on|off - Lägg till bindestreck i ISSN
* issndehyphenate=on|off - Ta bort bindestreck i ISSN
* bibupdate=on|off - Avgör om uppdateringar av bibliografiska poster ska leda till export
* holdtype=interleaved|after - Avgör om beståndsinformation ska vara inbakad i bib-posten eller följa med som separat därefter
* holdcreate=on|off - Avgör om nyskapade bestånd ska leda till export
* characterencoding=UTF-8|ISO-8859-1 - Avgör tecken-kodning
* biboperators=[list av sigel] - Ändringar av bibliografiska poster ska bara resultera i export om de gjorts av någon av följande sigel (lämnas tom för "alla")
* move0359=on|off - Flytta värde ifrån 035$9 till 035$a
* authupdate=on|off - Avgör om uppdateringar av auktoritetsposter ska leda till export
* sab=on|off - Avgör om SAB-titlar ska läggas till
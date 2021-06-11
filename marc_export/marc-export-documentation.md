# Dokumentation för Libris API för MARC21-export

Libris API för MARC21-export kan nås på:

https://libris.kb.se/api/marc_export/

För att exportera MARC-data skickas ett HTTP POST-anrop till ovanstående URL med en "export-profil" som meddelandekropp samt fyra parametrar i URL:en.

Dessa parametrar är:

1. `from` anger början på det tidsintervall för vilket man vill ha uppdateringar. Tidsangivelsen ska vara i format ISO-8601.
1. `until` anger slutet på det tidsintervall för vilket man vill ha uppdateringar. Tidsangivelsen ska vara i format ISO-8601.
1. `deleted` anger hur man vill att borttagningar ska hanteras. Parametern kan något utav värdena:
   1. `ignore` som innebär att borttagna poster helt enkelt ignoreras
   1. `export` som innebär att borttagna poster exporteras men är markerade som borttagna i MARC-leadern
   1. `append` som innebär att borttagna posters ID:n exporteras som en CSV-fil _efter_ den vanliga exportdatan (separerat av en null-byte).
1. `virtualDelete` som kan ha värde `true` eller `false`. Är `virtualDelete` satt till `true` så kommer poster anses vara borttagna i den genererade exporten, i de fall där de sigel som anges i `locations` i profilen inte längre har bestånd på posterna. Flaggan används förslagsvis tillsammans med `deleted=export`.

Exempel på anrop:
```
$ curl -Ss -XPOST "https://libris.kb.se/api/marc_export/?from=2019-10-05T22:00:00Z&until=2019-10-06T22:00:00Z&deleted=ignore&virtualDelete=false" --data-binary @./etc/export.properties > export.marc
```

SE UPP med era tidsangivelser/tidszoner! Exemplet ovan skickar in tider i UTC (därav 'Z' på slutet). Det är ett bra sätt att göra det på. Vill man skicka in lokala tider istället för UTC så går det också, men då måste tidszonen ingå i angivelsen. Vänligen läs på om ISO-8601!

Vill man anropa detta API med ett schemalagt skript så finns exempel/förslag på sådana skript här för:
[Windows](https://github.com/libris/librisxl/blob/master/marc_export/examplescripts/export_windows.bat)
och
[*nix-derivat (bash)](https://github.com/libris/librisxl/blob/master/marc_export/examplescripts/export_nix.sh)


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

| parameter            | giltiga värden             | beskrivning |
| -------------------- | -------------------------- | ----------- |
| `authcreate`         | `on`\|`off`                | Ska nyskapade auktoritetsposter resultera i export.
| `authoperators`      | [lista med sigel]          | Blankstegsseparerad lista. Ändringar av auktoritetsposter ska bara resultera i export om de gjorts av någon av följande sigel (lämnas tom för "alla").
| `authtype`           | `interleaved`\|`after`     | Avgör om auktoritetsinformation ska vara inbakad i bib-posten eller följa med som separat därefter.
| `authupdate`         | `on`\|`off`                | Avgör om uppdateringar av auktoritetsposter ska leda till export.
| `bibcreate`          | `on`\|`off`                | Avgör om nyskapande av bib-post ska resultera i export.
| `biboperators`       | [lista med sigel]          | Blankstegsseparerad lista. Ändringar av bibliografiska poster ska bara resultera i export om de gjorts av någon av följande sigel (lämnas tom för "alla").
| `bibupdate`          | `on`\|`off`                | Ska uppdateringar av bibliografiska poster leda till export.
| `characterencoding`  | `UTF-8`\|`ISO-8859-1`      | Avgör teckenkodning.
| `composestrategy`    | `compose`\|`decompose`     | Avgör ifall unicode-tecken ska vara composed eller decomposed.
| `exportdeleted`      | `on`\|`off`                | Ska borttagna poster exporteras (men markerade som borttagna i MARC-leadern). Motsvarar HTTP-parametern `deleted=export` resp. `deleted=ignore` som prioriteras framför värdet här om båda anges. Se även `virtualdelete`.
| `f003`               | [sträng]                   | Blankstegsseparerad lista. Tvinga fält 003 att anta ett visst värde.
| `format`             | `ISO2709`\|`MARCXML`       | Avgör serialiseringsformat för MARC-data.
| `holdcreate`         | `on`\|`off`                | Ska nyskapade bestånd leda till export.
| `holddelete`         | `on`\|`off`                | Ska borttagning av bestånd resultera i export.
| `holdoperators`      | [lista med sigel]          | Blankstegsseparerad lista. Ändringar av beståndsposter ska bara resultera i export om de gjorts av någon av följande sigel (lämnas tom för "alla").
| `holdtype`           | `interleaved`\|`after`     | Ska beståndsinformation vara inbakad i bib-posten eller följa med som separat därefter.
| `holdupdate`         | `on`\|`off`                | Ska uppdateringar av bestånd leda till export.
| `isbndehyphenate`    | `on`\|`off`                | Ta bort bindestreck i ISBN eller ej.
| `isbnhyphenate`      | `on`\|`off`                | Lägg till bindestreck i ISBN eller ej.
| `issndehyphenate`    | `on`\|`off`                | Ta bort bindestreck i ISSN.
| `issnhyphenate`      | `on`\|`off`                | Lägg till bindestreck i ISSN.
| `lcsh`               | `on`\|`off`                | Generera LCSH-fält i posten.
| `locations`          | [lista med sigel]\|`*`     | Blankstegsseparerad lista. Generera export för dessa sigel (alternativt * för alla sigel).
| `move0359`           | `on`\|`off`                | Flytta värde ifrån 035$9 till 035$a.
| `move240to244`       | `off`|`on`                 | Flytta MARC-fältet 240 till 244.
| `nameform`           | `Forskningsbiblioteksform` | Tvinga namnformer att anta Forskningsbiblioteksform.
| `sab`                | `on`\|`off`                | Ska SAB-titlar (976) läggas till.
| `virtualdelete`      | `on`\|`off`                | Betrakta bibliografiska poster som borttagna när de sigel som anges i `locations` inte längre har något bestånd. Används förslagsvis tillsammans med `exportdeleted`. Motsvarar HTTP-parametern `virtualDelete` som prioriteras framför värdet här om båda anges.

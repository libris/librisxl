# Exempelfrågor SPARQL

----

Nedan följer ett antal exempelfrågor som demonstrerar hur vår [SPARQL endpoint](https://libris.kb.se/sparql/) kan användas för uttag av statistik från Libris XL. 
Vill man förstå syntaxen kan man med fördel läsa W3C:s [specifikation](https://www.w3.org/TR/sparql11-query/) av SPARQL. 
Förekommande klasser och egenskaper står att finna i [vokabulären](https://id.kb.se/vocab/). 
Samtliga namnrymdsprefix som används är [fördefinierade](https://libris.kb.se/sparql/?help=nsdecl) i endpointen och behöver därför inte anges explicit i frågorna.

---

* #### Hur många romaner gavs ut under 2019?

        SELECT COUNT(DISTINCT ?novel) AS ?count {
            ?novel bf2:instanceOf/bf2:genreForm/(owl:sameAs|skos:exactMatch)* marc:Novel ;
                kbv:publication/kbv:year "2019"
        }

   **Kommentar:**  
   Här frågar vi efter antalet _instanser_ vilket innebär att t.ex. olika bandtyper räknas individuellt. 
   Att räkna antalet unika verk är tyvärr inte möjligt i dagsläget.  
   För att göra frågan beständig har `(owl:sameAs|skos:exactMatch)*` inkluderats så att alla klasser likvärdiga `marc:Novel` matchas. 
   På så sätt kan frågan hålla även om vi går ifrån använda marc-termen, förutsatt att denna finns kvar och refereras av likvärdiga klasser med just `owl:sameAs` eller `skos:exactMatch`.

---

 * #### Vilka språk finns Selma Lagerlöf översatt till?

        SELECT DISTINCT ?language ?langName {
            [] bf2:contribution [ a kbv:PrimaryContribution ;
                    bf2:role rel:author ;
                    bf2:agent <https://libris.kb.se/qn247n18248vs58#it> ] ;
                bf2:translationOf/a bf2:Work ;
                bf2:language ?language .
            ?language skos:prefLabel ?langName
            FILTER(lang(?langName) = 'sv')    
        }

    **Kommentar:**  
    Här förutsätter vi att författaren alltid ligger som länkad entitet under 'agent'. En variant för att matcha även
     lokala entiteter vore att byta ut URI:n `<https://libris.kb.se/qn247n18248vs58#it>` mot en blanknod `[ foaf:givenName
      "Selma" ; foaf:familyName "Lagerlöf" ]`. Detta fungerar dock dåligt i det fall författaren har ett mer generiskt
       namn.  

 ---

 * #### Vilka språk har svensk utgivning översatts till mellan åren 2000-2010?

        SELECT DISTINCT ?language ?langName {
            [] bf2:instanceOf [ bf2:language ?language ;
                        bf2:translationOf/bf2:language lge:swe ] ;
                kbv:publication/kbv:year ?year .
            ?language skos:prefLabel ?langName
            FILTER(str(?year) >= "2000" && str(?year) < "2010")
            FILTER(lang(?langName) = 'sv')
        }

    **Kommentar:**  
    Denna fråga har omtolkats till "Vilka språk har svenska titlar översatts till mellan åren 2000-2010?".
    Vi kan nämligen ta reda på verkets originalspråk via 'translationOf', däremot inget om dess
     originalutgivning.

 ---

  * #### Vilka svenska skönlitterära titlar har översatts till spanska 1990?

        SELECT ?spanishInstance ?spanishTitle ?swedishTitle {
            VALUES ?genre {
                marc:FictionNotFurtherSpecified
                marc:Drama
                marc:Essay
                marc:Novel
                marc:HumorSatiresEtc
                marc:Letter
                marc:ShortStory
                marc:MixedForms
                marc:Poetry
            }
            ?spanishInstance kbv:publication/kbv:year "1990" ;
                bf2:instanceOf ?work .
            ?work bf2:genreForm/(owl:sameAs|skos:exactMatch)* ?genre ;
                bf2:language lge:spa ;
                bf2:translationOf [ a bf2:Work ;
                        bf2:language lge:swe ]
            OPTIONAL {
                ?spanishInstance bf2:title [ a bf2:Title ;
                        bf2:mainTitle ?spanishTitle ]
            }
            OPTIONAL {
                ?work bf2:title [ a bf2:Title ;
                        bf2:mainTitle ?swedishTitle ]
            }
        }     

     **Kommentar:**  
     Tyvärr är det inte möjligt att få fram vilka svenska verk som översatts till spanska då det kräver att verket
      ligger länkat under 'translationOf'. I dagsläget får vi nöja oss med spanska instanser som översatts _från_
       svenska.  
       Önskvärt vore såklart också att ha en superklass som representerar all skönlitteratur. Visserligen finns
        `https://id.kb.se/term/saogf/Skönlitteratur` men den används idag i mindre utsträckning och dess relation
         till marc-termerna är inte heller definierad.

  ---

  * #### Vilka serietecknare har översatts till svenska under 1980-2020?

        SELECT DISTINCT ?cartoonist CONCAT(?givenName, " ", ?familyName) {
            VALUES ?genre {
                marc:ComicStrip
                marc:ComicOrGraphicNovel
            }
            [] bf2:instanceOf [ bf2:genreForm/(owl:sameAs|skos:exactMatch)* ?genre ;
                    bf2:language lge:swe ;
                    bf2:translationOf/a bf2:Work ;
                    bf2:contribution [ a kbv:PrimaryContribution ;
                            bf2:agent ?cartoonist ] ] ;
                kbv:publication/kbv:year ?year   
            OPTIONAL {
                ?cartoonist foaf:givenName ?givenName ;
                    foaf:familyName ?familyName
            }    
            FILTER(str(?year) >= "1980" && str(?year) < "2020")
            FILTER(!isBlank(?cartoonist))
        }

    **Kommentar:**  
    Serietecknare som ligger som lokala entiteter (blanknoder) under 'agent' filtreras här bort. Vill man ha med även
     de lokala entiteterna i resultatet tar man med fördel bort `FILTER(!isBlank(?cartoonist))`, dock innebär detta att
      samma serietecknare kan förekomma flera gånger.

----

* #### Hur många franska barnböcker översättes till svenska under 1980-2020?

        SELECT COUNT(DISTINCT ?book) AS ?count {
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf [ a bf2:Text ;
                        bf2:intendedAudience/(owl:sameAs|skos:exactMatch)* marc:Juvenile ;
                        bf2:language lge:swe ;
                        bf2:translationOf [ a bf2:Work ;
                                bf2:language lge:fre ] ] ;
                kbv:publication/kbv:year ?year
            FILTER(str(?year) >= "1980" && str(?year) < "2020")
        }    

    **Kommentar:**  
    Här frågar vi snarare efter antalet svenska resurser som översatts _från_ franska. Det omvända kräver att verken
    ligger länkade under 'translationOf', vilket inte är fallet i dagsläget.  
    Vi frågar heller inte uteslutande efter böcker. Det är inte möjligt då det saknas en generell struktur som
     indikerar att en resurs är specifikt en bok. Däremot är det fullt möjligt att begränsa frågan till monografier
      (instansen) av typen text (verket).

 ---

 * #### Hur många böcker gavs ut på samiska utifrån aspekterna genre, målgrupp och utgivningsår?

        SELECT ?year ?audience ?genre COUNT(?book) AS ?count {
            VALUES ?language {
                lge:smi
                lge:smj
                lge:sme
                lge:sjd
                lge:sju
                lge:sma
                lge:smn
                lge:sje
                lge:sia
                lge:sjt
                lge:sms
                lge:sjk
            }
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf [ a bf2:Text ;
                        bf2:language ?language ;
                        bf2:intendedAudience ?audience ;
                        bf2:genreForm ?genre ] ;
                kbv:publication/kbv:year ?year
            FILTER(!isBlank(?genre))
        }
        ORDER BY ?year ?audience ?genre

    **Kommentar:**  
    Det finns ingen URI som representerar alla samiska språk, utan vi får inkludera samtliga varieteter.

---

* #### Hur många facklitterära böcker gav förlaget Natur och Kultur ut mellan åren 1920-2000?

        SELECT COUNT(DISTINCT ?book) AS ?count {
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf [ a bf2:Text ;
                        bf2:genreForm/(owl:sameAs|skos:exactMatch)* marc:NotFictionNotFurtherSpecified ] ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        bf2:agent/rdfs:label ?agent ;
                        kbv:year ?year ]
            FILTER(regex(?agent, "Natur (&|och) Kultur|^N&K$", "i"))            
            FILTER(str(?year) >= "1920" && str(?year) < "2000")
        }

    **Kommentar:**  
    I brist på en klass som representerar facklitteratur får vi här använda `marc:NotFictionNotFurtherSpecified` (=Ej
     skönlitterärt verk). Önskvärt skulle vara att referera till URI:n som representerar Natur & Kultur men eftersom
      utgivare sällan är länkade får vi istället använda literaler som matchar de lokala entiteterna/blanknoderna.

---

* #### Hur många böcker ges ut av egenutgivare varje år?

        SELECT ?year COUNT(DISTINCT ?book) AS ?count {
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf/a bf2:Text ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        kbv:year ?year ] ;
                ^bf2:itemOf/kbv:cataloguersNote "nbegenutg"
        }
        ORDER BY ?year

---

* #### Hur många böcker har det getts ut inom barnlitteratur i Sverige varje år?

        SELECT ?year COUNT(DISTINCT ?book) AS ?count {
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf [ a bf2:Text ;
                        bf2:intendedAudience/(owl:sameAs|skos:exactMatch)* marc:Juvenile ] ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        kbv:country ctry:sw ;
                        kbv:year ?year ]
        }
        ORDER BY ?year

    **Kommentar:**  
    Vill man slippa "konstiga" årtal, dvs årtal som inte är på formen "yyyy", kan man lägga till det här filtret: `FILTER(regex(?year, "^[0-9]{4}$"))`

---

* #### Hur många böcker ges ut i Sverige totalt varje år?

        SELECT ?year COUNT(DISTINCT ?book) AS ?count {
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf/a bf2:Text ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        kbv:country ctry:sw ;
                        kbv:year ?year ]
        }
        ORDER BY ?year

---

* #### Hur många böcker har digitaliserats under 2020?

        SELECT COUNT(DISTINCT ?digiBook) AS ?count {
            ?digiBook bf2:issuance kbv:Monograph ;
                bf2:instanceOf/a bf2:Text ;
                kbv:publication/kbv:year "2020" ;
                ^foaf:primaryTopic/kbv:bibliography <https://libris.kb.se/library/DIGI>
        }

    **Kommentar:**
    Än så länge är det mest effektiva (men tyvärr något konstlade) sättet att avgöra om något digitaliserats att söka efter
     sigel "DIGI" via entitetens associerade named graph (record). Så småningom ska det istället bli möjligt att
      finna denna information direkt på entiteten själv medelst formen `?digiBook kbv:production [ a kbv:DigitalReproduction ]`.
      Dessutom ska man kunna få fram den fysiska versionen via `?digiBook bf2:reproductionOf ?book`.

---

* #### Vilka titlar digitaliserades 2019?

        SELECT ?digi ?title {
            ?digi kbv:publication/kbv:year "2019" ;
                ^foaf:primaryTopic/kbv:bibliography [ a sdo:Library ;
                        kbv:sigel "DIGI" ]
            OPTIONAL {
                ?digiBook bf2:title [ a bf2:Title
                        bf2:mainTitle ?title ]
            }
        }

---

* #### Hur många svenska utgivare fanns det 1970?

        SELECT COUNT(DISTINCT ?publisher) AS ?count {
            [] a kbv:PrimaryPublication ;
                kbv:country ctry:sw ;
                kbv:year "1970" ;
                bf2:agent/rdfs:label ?publisher
        }

    **Kommentar:**  
    Här frågar vi snarare "Vilka svenska utgivare gav ut något under 1970?". För att besvara originalfrågan hade
     utgivare behövt ligga länkat under 'agent' så att vi kunnat finna mer info om denna, typ 'establishDate'. I
      dagsläget är utgivare mestadels lokala entiteter. Det gör också att vi inte kan räkna entiteterna själva
       eftersom samma utgivare då riskerar att räknas flera gånger. Istället bör unika 'labels' ge det mest exakta
        resultatet, även om det kan förekomma olika labels på samma utgivare, typ "Natur & Kultur" och "N&K".

---

* #### Hur många barnböcker gavs ut på ett annat språk än svenska av svenska utgivare 2019?

        SELECT COUNT(DISTINCT ?book) AS ?count {
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf [ a bf2:Text ;
                        bf2:intendedAudience/(owl:sameAs|skos:exactMatch)* marc:Juvenile ;
                        bf2:language ?language ] ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        kbv:country ctry:sw ;
                        kbv:year "2019" ]
            FILTER(?language != lge:swe)
        }

---

* #### Vilka titlar har getts ut om coronapandemin 2019-2020 och coronaviruset?

        SELECT DISTINCT ?instance ?title {
            VALUES ?subject {
                sao:Covid-19
                sao:Coronapandemin%202019-2020%20
                sao:Coronavirus
            }
            ?instance bf2:instanceOf/bf2:subject ?subject
            OPTIONAL {
                ?instance bf2:title [ a bf2:Title ;
                        bf2:mainTitle ?title ]
            }
        }

---

* #### Hur många titlar har getts ut om coronapandemin 2019-2020 och coronaviruset?

        SELECT COUNT(DISTINCT ?instance) {
            VALUES ?subject {
                sao:Covid-19
                sao:Coronapandemin%202019-2020%20
                sao:Coronavirus
            }
            ?instance bf2:instanceOf/bf2:subject ?subject
        }

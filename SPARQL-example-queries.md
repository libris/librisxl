# Exempelfrågor SPARQL

----

Detta dokument innehåller ett antal exempelfrågor som demonstrerar hur vår [SPARQL endpoint](https://libris.kb.se/sparql/) 
kan användas för uttag av statistik från Libris XL. 
Vill man förstå syntaxen kan man med fördel läsa W3C:s [specifikation](https://www.w3.org/TR/sparql11-query/) av SPARQL. 

Datat är huvudsakligen uttryckt i [Bibframe](http://id.loc.gov/ontologies/bibframe/), med kompletterande termer
 från andra välkända vokabulär samt vår egen KBV. Samtliga förekommande termer från
  externa vokabulär finns mappade till ekvivalenta termer i KBV. För att veta vilka termer att använda kan man slå 
  upp dessa mappningar, antingen via [webbgränssnittet](https://id.kb.se/vocab/) eller direkt i en SPARQL-fråga med
   (exempel): 
   
   `kbv:SomeClass owl:equivalentClass ?mappedClass` 
   
   och 
   
   `kbv:someProperty owl:equivalentProperty ?mappedProperty` 
   
   för klasser respektive egenskaper.
   
Vanliga [namnrymdsprefix](https://www.w3.org/TR/sparql11-query/#prefNames) är [fördefinierade](https://libris.kb.se/sparql/?help=nsdecl) 
i endpointen och behöver därför inte deklareras explicit i frågorna.

---

* #### Hur många romaner gavs ut under 2019?

        define input:same-as "yes"
        
        SELECT (COUNT(DISTINCT ?novel) as ?count) {
            ?gf (madsrdf:hasExactExternalAuthority|^madsrdf:hasExactExternalAuthority)* marc:Novel .
            ?novel bf2:instanceOf/bf2:genreForm ?gf ;
                kbv:publication/kbv:year "2019" .
        }

   **Kommentar:**  
   Här frågar vi efter antalet _instanser_ vilket innebär att t.ex. olika bandtyper räknas individuellt. 
   Att räkna antalet unika verk är tyvärr inte möjligt i dagsläget, eftersom de flesta verk saknar URI:er att referera
    till.
   
   I nuvarande data identifierar vi romaner med genre/form-termen `marc:Novel`, men för att göra frågan beständig
    matchar vi även ekvivalenta termer. För ekvivalenta termer sammanlänkade med `owl:sameAs` kan vi ange
     `define input:same-as "yes"`; sådana länkar härleds då implicit. Att `exactMatch`-länkade termer
      till/från `marc:Novel` ska matchas behöver anges explicit i trippelmönstret.
   
   I denna fråga och många av de följande används s.k. [property paths](https://www.w3.org/TR/sparql11-query/#propertypaths) 
   för att inte behöva skriva ut varje trippel i sin helhet.
   
---

 * #### Vilka språk finns Selma Lagerlöf översatt till?

        SELECT DISTINCT ?language ?langName {
            [] bf2:contribution [ a kbv:PrimaryContribution ;
                    bf2:role rel:author ;
                    bf2:agent <https://libris.kb.se/qn247n18248vs58#it> ] ;
                bf2:translationOf/a bf2:Work ;
                bf2:language ?language .
            ?language skos:prefLabel ?langName .
            
            FILTER(lang(?langName) = 'sv')    
        }

    **Kommentar:**  
    Här förutsätter vi att författaren alltid ligger som länkad entitet under `bf2:agent`. En variant för att matcha
     även lokala entiteter vore att byta ut URI:n `<https://libris.kb.se/qn247n18248vs58#it>` mot en blanknod `[ foaf
     :givenName "Selma" ; foaf:familyName "Lagerlöf" ]`. Detta fungerar dock dåligt i det fall författaren har ett
      mer generiskt namn.  

 ---

 * #### Vilka språk har svensk utgivning översatts till mellan åren 2000-2010?

        SELECT DISTINCT ?language ?langName {
            [] bf2:instanceOf [ bf2:language ?language ;
                    bf2:translationOf/bf2:language lge:swe ] ;
                kbv:publication/kbv:year ?year .
            ?language skos:prefLabel ?langName .
            
            FILTER(str(?year) >= "2000" && str(?year) < "2010")
            FILTER(lang(?langName) = 'sv')
        }

    **Kommentar:**  
    Denna fråga har omtolkats till "Vilka språk har svenska titlar översatts till mellan åren 2000-2010?".
    Vi kan nämligen ta reda på verkets originalspråk via `bf2:translationOf`, däremot inget om dess originalutgivning.

 ---

  * #### Vilka svenska skönlitterära titlar har översatts till spanska 1990?
  
        define input:same-as "yes"

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
            ?work bf2:genreForm/(madsrdf:hasExactExternalAuthority|^madsrdf:hasExactExternalAuthority)* ?genre ;
                bf2:language lge:spa ;
                bf2:translationOf [ a bf2:Work ;
                        bf2:language lge:swe ] .
            OPTIONAL {
                ?spanishInstance bf2:title [ a bf2:Title ;
                        bf2:mainTitle ?spanishTitle ] .
            }
            OPTIONAL {
                ?work bf2:title [ a bf2:Title ;
                        bf2:mainTitle ?swedishTitle ] .
            }
        }     

     **Kommentar:**  
     Tyvärr är det inte möjligt att få fram vilka svenska verk som översatts till spanska då det kräver att verket
      ligger länkat under `bf2:translationOf`. I dagsläget får vi nöja oss med spanska instanser som översatts _från_
       svenska.  
       Det vore även önskvärt att ha en superklass som representerar all skönlitteratur. Visserligen finns
        `saogf:Skönlitteratur` men den används idag i mindre utsträckning och dess relation
         till marc-termerna är inte heller definierad.

  ---

  * #### Vilka serietecknare har översatts till svenska under 1980-2020?
        
        define input:same-as "yes"

        SELECT DISTINCT ?cartoonist (CONCAT(?givenName, " ", ?familyName) as ?name) {
            VALUES ?genre {
                marc:ComicStrip
                marc:ComicOrGraphicNovel
            }
            
            ?gf (madsrdf:hasExactExternalAuthority|^madsrdf:hasExactExternalAuthority)* ?genre .
            [] bf2:instanceOf [ bf2:genreForm ?gf ;
                    bf2:language lge:swe ;
                    bf2:translationOf/a bf2:Work ;
                    bf2:contribution [ a kbv:PrimaryContribution ;
                            bf2:agent ?cartoonist ] ] ;
                kbv:publication/kbv:year ?year .   
            OPTIONAL {
                ?cartoonist foaf:givenName ?givenName ;
                    foaf:familyName ?familyName .
            }    
            
            FILTER(str(?year) >= "1980" && str(?year) < "2020")
            FILTER(isIri(?cartoonist))
        }

    **Kommentar:**  
    Serietecknare som ligger som lokala entiteter (blanknoder) under `bf2:agent` filtreras här bort. Vill man ha med
     även
     de lokala entiteterna i resultatet tar man med fördel bort `FILTER(!isIri(?cartoonist))`, dock innebär detta att
      samma serietecknare kan förekomma flera gånger i resultatet.

----

* #### Hur många franska barnböcker översättes till svenska under 1980-2020?
        
        define input:same-as "yes"

        SELECT (COUNT(DISTINCT ?book) AS ?count) {
            ?audience (madsrdf:hasExactExternalAuthority|^madsrdf:hasExactExternalAuthority)* marc:Juvenile .
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf [ a bf2:Text ;
                        bf2:intendedAudience ?audience ;
                        bf2:language lge:swe ;
                        bf2:translationOf [ a bf2:Work ;
                                bf2:language lge:fre ] ] ;
                kbv:publication/kbv:year ?year .
            
            FILTER(str(?year) >= "1980" && str(?year) < "2020")
        }    

    **Kommentar:**  
    Här frågar vi snarare efter antalet svenska resurser som översatts _från_ franska. Det omvända kräver att verken
    ligger länkade under `bf2:translationOf`, vilket inte är fallet i dagsläget.  
    Vi frågar heller inte uteslutande efter böcker. Det är inte möjligt då det saknas en generell struktur som
     indikerar att en resurs är specifikt en bok. Däremot är det fullt möjligt att begränsa frågan till monografier
      (instansen) av typen text (verket).

 ---

 * #### Hur många böcker gavs ut på samiska utifrån aspekterna genre, målgrupp och utgivningsår?
        
        SELECT ?year ?audience ?genre (COUNT(?book) AS ?count) {
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
                kbv:publication/kbv:year ?year .
            
            FILTER(!isIri(?genre))
        }
        GROUP BY ?year ?audience ?genre
        ORDER BY ?year ?audience ?genre

    **Kommentar:**  
    Det finns ingen URI som representerar alla samiska språk, utan vi får inkludera samtliga varieteter.

---

* #### Hur många facklitterära böcker gav förlaget Natur och Kultur ut mellan åren 1920-2000?
        
        define input:same-as "yes"

        SELECT (COUNT(DISTINCT ?book) AS ?count) {
            ?gf (madsrdf:hasExactExternalAuthority|^madsrdf:hasExactExternalAuthority)* marc:NotFictionNotFurtherSpecified . 
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf [ a bf2:Text ;
                        bf2:genreForm marc:NotFictionNotFurtherSpecified ] ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        bf2:agent/rdfs:label ?agentLabel ;
                        kbv:year ?year ] .
            
            FILTER(regex(?agentLabel, "Natur (&|och) Kultur|^N&K$", "i"))            
            FILTER(str(?year) >= "1920" && str(?year) < "2000")
        }

    **Kommentar:**  
    I brist på en klass som representerar facklitteratur får vi här använda `marc:NotFictionNotFurtherSpecified` (=Ej
     skönlitterärt verk). Det vore önskvärt att kunna referera till en URI som representerar förlaget Natur & Kultur
      men eftersom utgivare sällan är länkade får vi istället matcha de lokala entiteterna / blanknoderna på dess 
      benämning.

---

* #### Hur många böcker ges ut av egenutgivare varje år?

        SELECT ?year (COUNT(DISTINCT ?book) AS ?count) {
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf/a bf2:Text ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        kbv:year ?year ] ;
                ^bf2:itemOf/kbv:hasComponent?/kbv:cataloguersNote "nbegenutg" .
        }
        ORDER BY ?year

---

* #### Hur många böcker har det getts ut inom barnlitteratur i Sverige varje år?

        define input:same-as "yes"

        SELECT ?year (COUNT(DISTINCT ?book) AS ?count) {
            ?audience (madsrdf:hasExactExternalAuthority|^madsrdf:hasExactExternalAuthority)* marc:Juvenile .
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf [ a bf2:Text ;
                        bf2:intendedAudience marc:Juvenile ] ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        kbv:country ctry:sw ;
                        kbv:year ?year ] .
        }
        GROUP BY ?year
        ORDER BY ?year

    **Kommentar:**  
    Vill man undanta årtal som avviker från formen "yyyy" kan man lägga till det här filtret: `FILTER(regex(?year
    , "^[0-9]{4}$"))`.
    
---

* #### Hur många böcker ges ut i Sverige totalt varje år?

        SELECT ?year (COUNT(DISTINCT ?book) AS ?count) {
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf/a bf2:Text ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        kbv:country ctry:sw ;
                        kbv:year ?year ] .
        }
        GROUP BY ?year
        ORDER BY ?year

---

* #### Hur många böcker har digitaliserats under 2020?

        SELECT (COUNT(DISTINCT ?digiBook) AS ?count) {
            ?digiBook bf2:issuance kbv:Monograph ;
                bf2:instanceOf/a bf2:Text ;
                kbv:production/bf2:date "2020" ;
                ^foaf:primaryTopic/kbv:bibliography lib:DIGI .
        }
     
---

* #### Vilka titlar digitaliserades 2019?

        SELECT DISTINCT ?digi ?title {
            ?digi kbv:production/bf2:date "2019" ;
                ^foaf:primaryTopic/kbv:bibliography lib:DIGI .
            OPTIONAL {
                ?digi bf2:title [ a bf2:Title ;
                        bf2:mainTitle ?title ] .
            }
        }
        ORDER BY ?title
    
---

* #### Hur många svenska utgivare fanns det 1970?

        SELECT (COUNT(DISTINCT ?publisherLabel) AS ?count) {
            [] a kbv:PrimaryPublication ;
                kbv:country ctry:sw ;
                kbv:year "1970" ;
                bf2:agent/bf2:label ?publisherLabel .
        }

    **Kommentar:**  
    Här frågar vi snarare "Vilka utgivare gav ut något i Sverige under 1970?". Tillräckliga påståenden om utgivare för
     att besvara originalfrågan saknas i dagsläget. Utgivare ligger mestadels som lokala entiteter, där enbart dess
      benämningar anges. Här skulle vi istället vilja att utgivare tilldelats URI:er och länkats under `bf2:agent`. 
      På respektive URI skulle sedan relevant information kunna samlas, exempelvis när förlaget grundats och landet
       det verkar i.
    
    Om utgivare var länkade skulle vi också få ett mer exakt resultat, tack vare att vi då skulle kunna garantera att
     antalet _unika_ utgivare räknas. Att räkna blanknoder fungerar inte eftersom vi inte kan särskilja vilka som
      representerar samma förlag. Istället räknar vi antalet unika benämningar, även om inte heller detta sätt
       garanterar ett helt exakt resultat då det kan förekomma olika benämningar på samma förlag, t.ex. "Natur
        & Kultur" och "N&K". 

---

* #### Hur många barnböcker gavs ut på ett annat språk än svenska av svenska utgivare 2019?
        
        define input:same-as "yes"

        SELECT (COUNT(DISTINCT ?book) AS ?count) {
            ?audience (madsrdf:hasExactExternalAuthority|^madsrdf:hasExactExternalAuthority)* marc:Juvenile .
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf [ a bf2:Text ;
                        bf2:intendedAudience marc:Juvenile ;
                        bf2:language ?language ] ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        kbv:country ctry:sw ;
                        kbv:year "2019" ] .
            
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
            
            ?instance bf2:instanceOf/bf2:subject ?subject .
            OPTIONAL {
                ?instance bf2:title [ a bf2:Title ;
                        bf2:mainTitle ?title ] .
            }
        }

---

* #### Hur många titlar har getts ut om coronapandemin 2019-2020 och coronaviruset?

        SELECT (COUNT(DISTINCT ?instance) AS ?count) {
            VALUES ?subject {
                sao:Covid-19
                sao:Coronapandemin%202019-2020%20
                sao:Coronavirus
            }
            
            ?instance bf2:instanceOf/bf2:subject ?subject
        }
        
---

* #### Hur många tryckta monografier katalogiserades av Kungliga biblioteket 2020?

        SELECT ?month (COUNT(?instance) as ?count) {  
            ?instance a bf2:Print ;
                bf2:issuance kbv:Monograph .
            ?hold bf2:itemOf ?instance ;
                bf2:heldBy lib:S .
            ?holdMeta foaf:primaryTopic ?hold ;
                bf2:creationDate ?date .
            
            BIND(month(?date) as ?month)
            FILTER(year(?date) = 2020)
        }
        GROUP BY ?month
        ORDER BY ?month
    
    **Kommentar:**
    Med katalogiserades menar vi här när beståndspost skapades. Svaret visar antal per månad.
    
---

* #### Hur många elektroniska seriella resurser katalogiserades av Kungliga biblioteket 2018?
        
        SELECT ?month (COUNT(?instance) as ?count) {  
            ?instance a bf2:Electronic ;
                bf2:issuance kbv:Serial .
            ?hold bf2:itemOf ?instance ;
                bf2:heldBy lib:S .
            ?holdMeta foaf:primaryTopic ?hold ;
                bf2:creationDate ?date .
          
            BIND(month(?date) as ?month)
            FILTER(year(?date) = 2018)
        }
        GROUP BY ?month
        ORDER BY ?month
        
---
        
* #### Hur många monografier inom DDK 320 katalogiserades av Umeå universitetsbibliotek 2019?

        SELECT COUNT(DISTINCT ?instance) {  
            ?instance bf2:issuance kbv:Monograph ;
                bf2:instanceOf/bf2:classification [ a bf2:ClassificationDdc ; 
                        bf2:code ?code ] .
            ?hold bf2:itemOf ?instance ;
                bf2:heldBy lib:Q .
            ?holdMeta foaf:primaryTopic ?hold ;
                bf2:creationDate ?date .
            
            FILTER(STRSTARTS(?code, "320"))
            FILTER(year(?date) = 2019)
        }
        
---

* #### Hur många poster katalogiserades med Svenska ämnesordet Missionärer 2010-2019?

        SELECT COUNT(DISTINCT ?instance) {  
            ?instance bf2:instanceOf/bf2:subject <https://id.kb.se/term/sao/Mission%C3%A4rer> ;
                ^bf2:itemOf ?hold .
            ?holdMeta foaf:primaryTopic ?hold ;
                bf2:creationDate ?date .
            
            FILTER(year(?date) >= 2010 && year(?date) <= 2020)
        }
        
---
 
 * #### Hur många poster finns det inom bibliografin SUEC??
 
        SELECT COUNT(?meta) {
            ?meta a kbv:Record ;
                kbv:bibliography lib:SUEC .
        } 
        
---
  
 * #### Hur många nya personbeskrivningar (auktoritetsposter) med ISNI skapades 2017-2021?
  
        SELECT COUNT(?person) WHERE {
            ?meta foaf:primaryTopic ?person ;
                bf2:creationDate ?date .
            ?person a bf2:Person ;
                bf2:identifiedBy [ a bf2:Isni ]
      
            FILTER(year(?date) >= 2017 && year(?date) <= 2021)
        }
        
---

 * #### Hur många personbeskrivningar ändrades 2017-2021?
 
        SELECT COUNT(?person) {
            ?meta foaf:primaryTopic ?person ;
                bf2:changeDate ?date .
            ?person a bf2:Person .
              
            FILTER(year(?date) >= 2017 && year(?date) <= 2021)
        }
        
    **Kommentar:**
    För att få motsvarande resultat för andra entitetstyper än personer räcker det att ändra `bf2:Person` till önskad
     typ, t.ex. `bf2:Organization`.
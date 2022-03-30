# Exempelfrågor SPARQL

----

Detta dokument innehåller ett antal exempelfrågor som demonstrerar hur vår [SPARQL endpoint](https://libris.kb.se/sparql/) kan användas för uttag av statistik från Libris XL.
Vill man förstå syntaxen kan man med fördel läsa [W3C:s specifikation av SPARQL](https://www.w3.org/TR/sparql11-query/).

Resurserna är beskrivna med termer ur KB:s egen [vokabulär](https://id.kb.se/vocab/) (KBV).
De flesta termerna i KBV är mappade till motsvarigheter i andra välkända vokabulär, huvudsakligen [Bibframe](http://id.loc.gov/ontologies/bibframe/).
Dessa mappningar kan man slå upp i [webbgränssnittet](https://id.kb.se/vocab/) eller direkt i en SPARQL-fråga med (exempel):

`kbv:ExampleClass kbv:equivalentClass ?mappedClass`

eller

`kbv:exampleProperty kbv:equivalentProperty ?mappedProperty`

för klasser respektive egenskaper.

Vanliga [namnrymdsprefix](https://www.w3.org/TR/sparql11-query/#prefNames) är [fördefinierade](https://libris.kb.se/sparql/?help=nsdecl) i endpointen.
Dessa kan användas i frågorna utan att behöva deklareras explicit.
För termer ur KBV fungerar det fördefinierade prefixet `kbv:`, men i exempelfrågorna som följer använder vi istället standardprefixet `:` (deklareras explicit) för bättre läsbarhet.

---

* #### Hur många romaner gavs ut i Sverige under 2019?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?novel) as ?count) {
            ?gf (:exactMatch|^:exactMatch|:sameAs|^:sameAs)* marc:Novel .
            ?novel :instanceOf/:genreForm ?gf ;
                :publication [ :year "2019" ; :country ctry:sw ] .
        }

   **Kommentar:**  
   Här frågar vi efter antalet _instanser_ vilket innebär att t.ex. olika bandtyper räknas individuellt.
   Att räkna antalet unika verk är tyvärr inte möjligt i dagsläget, eftersom verk inte har URI:er att referera till.

   I nuvarande data identifieras romaner med genre/form-termen `marc:Novel`, men för att göra frågan beständig matchar vi även ekvivalenta termer (länkade med `:sameAs` eller `:exactMatch`).

   I denna fråga och många av de följande används s.k. [property paths](https://www.w3.org/TR/sparql11-query/#propertypaths)
   för att inte behöva skriva ut varje trippel i sin helhet.

---

 * #### Vilka språk finns Selma Lagerlöf översatt till?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT DISTINCT ?language ?langName {
            [] :contribution [ a :PrimaryContribution ;
                    :role rel:author ;
                    :agent <https://libris.kb.se/qn247n18248vs58#it> ] ;
                :translationOf/a :Work ;
                :language ?language .
            ?language :prefLabel ?langName .

            FILTER(lang(?langName) = 'sv')    
        }

    **Kommentar:**  
    Här förutsätter vi att författaren alltid ligger som länkad entitet under `:agent`. En variant för att matcha även lokala entiteter vore att byta ut URI:n `<https://libris.kb.se/qn247n18248vs58#it>` mot en blanknod `[ :givenName "Selma" ; :familyName "Lagerlöf" ]`. Detta fungerar dock dåligt i det fall författaren har ett mer generiskt namn.  

 ---

 * #### Vilka språk har svensk utgivning översatts till mellan åren 2000-2010?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT DISTINCT ?language ?langName {
            [] :instanceOf [ :language ?language ;
                    :translationOf/:language lge:swe ] ;
                :publication/:year ?year .
            ?language :prefLabel ?langName .

            FILTER(str(?year) >= "2000" && str(?year) < "2010")
            FILTER(lang(?langName) = 'sv')
        }

    **Kommentar:**  
    Denna fråga har omtolkats till "Vilka språk har svenska titlar översatts till mellan åren 2000-2010?".
    Vi kan nämligen ta reda på verkets originalspråk via `bf2:translationOf`, däremot inget om dess originalutgivning.

 ---

  * #### Vilka svenska skönlitterära titlar har översatts till spanska 1990?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT DISTINCT ?spanishInstance ?spanishTitle ?swedishTitle {
            VALUES ?genreForm {
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

            ?gf (:exactMatch|^:exactMatch|:sameAs|^:sameAs)* ?genreForm .

            ?spanishInstance :publication/:year "1990" ;
                :instanceOf ?work .
            ?work :genreForm ?gf ;
                :language lge:spa ;
                :translationOf [ a :Work ; :language lge:swe ] .
            OPTIONAL {
                ?spanishInstance :hasTitle [ a :Title ; :mainTitle ?spanishTitle ] .
            }
            OPTIONAL {
                ?work :hasTitle [ a :Title ; :mainTitle ?swedishTitle ] .
            }
        }     

     **Kommentar:**  
     Tyvärr är det inte möjligt att få fram vilka svenska verk som översatts till spanska då det kräver att verket ligger länkat under `bf2:translationOf`. I dagsläget får vi nöja oss med spanska instanser som översatts _från_ svenska.

     Det vore även önskvärt att kunna ange _en_ term för _all_ skönlitteratur. Visserligen finns `saogf:Sk%C3%B6nlitteratur` men den har hittills för lite användning och dess relation till marc-termerna är inte heller definierad.

  ---

  * #### Vilka serietecknare har översatts till svenska under 1980-2020?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT DISTINCT ?cartoonist (CONCAT(?givenName, " ", ?familyName) as ?name) {
            VALUES ?genreForm {
                marc:ComicStrip
                marc:ComicOrGraphicNovel
            }

            ?gf (:exactMatch|^:exactMatch|:sameAs|^:sameAs)* ?genreForm .
            [] :instanceOf [ :genreForm ?gf ;
                    :language lge:swe ;
                    :translationOf/a :Work ;
                    :contribution [ a :PrimaryContribution ;
                            :agent ?cartoonist ] ] ;
                :publication/:year ?year .   
            OPTIONAL {
                ?cartoonist :givenName ?givenName ;
                    :familyName ?familyName .
            }    

            FILTER(str(?year) >= "1980" && str(?year) < "2020")
            FILTER(isIri(?cartoonist))
        }

    **Kommentar:**  
    Serietecknare som ligger som lokala entiteter (blanknoder) under `:agent` filtreras här bort. Vill man ha med även de lokala entiteterna i resultatet tar man med fördel bort `FILTER(isIri(?cartoonist))`, dock innebär detta att samma serietecknare kan förekomma flera gånger i resultatet.

----

* #### Hur många franska barnböcker översättes till svenska under 1980-2020?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?book) AS ?count) {
            ?audience (:exactMatch|^:exactMatch|:sameAs|^:sameAs)* marc:Juvenile .
            ?book :issuanceType :Monograph ;
                :instanceOf [ a :Text ;
                        :intendedAudience ?audience ;
                        :language lge:swe ;
                        :translationOf [ a :Work ;
                                :language lge:fre ] ] ;
                :publication/:year ?year .

            FILTER(str(?year) >= "1980" && str(?year) < "2020")
        }    

    **Kommentar:**  
    Här frågar vi snarare efter antalet svenska resurser som översatts _från_ franska. Det omvända kräver att verken ligger länkade under `:translationOf`, vilket inte är fallet i dagsläget.  
    Vi frågar heller inte uteslutande efter böcker. Det är inte möjligt då det saknas en generell struktur som indikerar att en resurs är specifikt en bok. Däremot är det fullt möjligt att begränsa frågan till monografier (instansen) av typen text (verket).

 ---

 * #### Hur många böcker gavs ut på samiska utifrån aspekterna genre, målgrupp och utgivningsår?

        PREFIX : <https://id.kb.se/vocab/>

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

            ?book :issuanceType :Monograph ;
                :instanceOf [ a :Text ;
                        :language ?language ;
                        :intendedAudience ?audience ;
                        :genreForm ?genre ] ;
                :publication/:year ?year .

            FILTER(isIri(?genre))
        }
        GROUP BY ?year ?audience ?genre
        ORDER BY ?year ?audience ?genre

    **Kommentar:**  
    Det finns ingen URI som representerar alla samiska språk, utan vi får inkludera samtliga varieteter.

---

* #### Hur många facklitterära böcker gav förlaget Natur och Kultur ut mellan åren 1920-2000?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?book) AS ?count) {
            VALUES ?agentLabel {
                "Natur & Kultur"
                "Natur & kultur"
                "Natur och kultur"
                "Natur och Kultur"
                "N&K"
            }
            ?gf (:exactMatch|^:exactMatch|:sameAs|^:sameAs)* marc:NotFictionNotFurtherSpecified .
            ?book :issuanceType :Monograph ;
                :instanceOf [ a :Text ;
                        :genreForm marc:NotFictionNotFurtherSpecified ] ;
                :publication [ a :PrimaryPublication ;
                        :agent/:label ?agentLabel ;
                        :year ?year ] .

            FILTER(str(?year) >= "1920" && str(?year) < "2000")
        }

    **Kommentar:**  
    I brist på en klass som representerar facklitteratur får vi här använda `marc:NotFictionNotFurtherSpecified` (=Ej skönlitterärt verk).
    Det vore önskvärt att kunna referera till en URI som representerar förlaget Natur & Kultur men eftersom utgivare inte är länkade får vi istället matcha lokala entiteter / blanknoder på benämning.

---

* #### Hur många böcker ges ut av egenutgivare varje år?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT ?year (COUNT(DISTINCT ?book) AS ?count) {
            ?book :issuanceType :Monograph ;
                :instanceOf/a :Text ;
                :publication [ a :PrimaryPublication ;
                        :year ?year ] ;
                ^:itemOf/:hasComponent?/:cataloguersNote "nbegenutg" .
        }
        ORDER BY ?year

---

* #### Hur många böcker har det getts ut inom barnlitteratur i Sverige varje år?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT ?year (COUNT(DISTINCT ?book) AS ?count) {
            ?audience (:exactMatch|^:exactMatch|:sameAs|^:sameAs)* marc:Juvenile .
            ?book :issuanceType :Monograph ;
                :instanceOf [ a :Text ;
                        :intendedAudience marc:Juvenile ] ;
                :publication [ a :PrimaryPublication ;
                        :country ctry:sw ;
                        :year ?year ] .
        }
        GROUP BY ?year
        ORDER BY ?year

    **Kommentar:**  
    Vill man undanta årtal som avviker från formen "yyyy" kan man lägga till det här filtret: `FILTER(regex(?year, "^[0-9]{4}$"))`.

---

* #### Hur många böcker ges ut i Sverige totalt varje år?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT ?year (COUNT(DISTINCT ?book) AS ?count) {
            ?book :issuanceType :Monograph ;
                :instanceOf/a :Text ;
                :publication [ a :PrimaryPublication ;
                        :country ctry:sw ;
                        :year ?year ] .
        }
        GROUP BY ?year
        ORDER BY ?year

---

* #### Hur många böcker har digitaliserats under 2020?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?digiBook) AS ?count) {
            ?digiBook :issuanceType :Monograph ;
                :instanceOf/a :Text ;
                :production/:date "2020" ;
                ^:mainEntity/:bibliography lib:DIGI .
        }

---

* #### Vilka titlar digitaliserades 2019?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT DISTINCT ?digi ?title {
            ?digi :production/:date "2019" ;
                ^:mainEntity/:bibliography lib:DIGI .
            OPTIONAL {
                ?digi :hasTitle [ a :Title ;
                        :mainTitle ?title ] .
            }
        }
        ORDER BY ?title

---

* #### Hur många svenska utgivare fanns det 1970?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?publisherLabel) AS ?count) {
            [] a :PrimaryPublication ;
                :country ctry:sw ;
                :year "1970" ;
                :agent/:label ?publisherLabel .
        }

    **Kommentar:**  
    Här frågar vi snarare "Vilka utgivare gav ut något i Sverige under 1970?". Tillräckliga påståenden om utgivare för att besvara originalfrågan saknas. Utgivare ligger mestadels som lokala entiteter, där enbart dess benämningar anges. Här skulle vi istället vilja att utgivare representerades av URI:er och   länkats under `:agent`. På respektive URI skulle sedan relevanta påståenden kunna samlas, exempelvis när förlaget grundats och landet det verkar i.

    Om utgivare var länkade skulle vi också få ett mer exakt resultat, tack vare att vi då skulle kunna garantera att antalet _unika_ utgivare räknas. Att räkna blanknoder fungerar inte eftersom vi inte kan särskilja vilka som representerar samma förlag. Istället räknar vi antalet unika benämningar, även om inte heller detta sätt garanterar ett helt exakt resultat då det kan förekomma olika benämningar på samma förlag, t.ex. "Natur & Kultur" och "N&K".

---

* #### Hur många barnböcker gavs ut på ett annat språk än svenska av svenska utgivare 2019?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?book) AS ?count) {
            ?audience (:exactMatch|^:exactMatch|:sameAs|^:sameAs)* marc:Juvenile .
            ?book :issuanceType :Monograph ;
                :instanceOf [ a :Text ;
                        :intendedAudience marc:Juvenile ;
                        :language ?language ] ;
                :publication [ a :PrimaryPublication ;
                        :country ctry:sw ;
                        :year "2019" ] .

            FILTER(?language != lge:swe)
        }

---

* #### Vilka titlar har getts ut om coronapandemin 2019-2020 och coronaviruset?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT DISTINCT ?instance ?title {
            VALUES ?subject {
                sao:Covid-19
                sao:Coronapandemin%202019-2020%20
                sao:Coronavirus
            }

            ?instance :instanceOf/:subject ?subject .
            OPTIONAL {
                ?instance :hasTitle [ a :Title ;
                        :mainTitle ?title ] .
            }
        }

---

* #### Hur många titlar har getts ut om coronapandemin 2019-2020 och coronaviruset?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?instance) AS ?count) {
            VALUES ?subject {
                sao:Covid-19
                sao:Coronapandemin%202019-2020%20
                sao:Coronavirus
            }

            ?instance :instanceOf/:subject ?subject
        }

---

* #### Hur många tryckta monografier katalogiserades av Kungliga biblioteket 2020?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT ?month (COUNT(?instance) AS ?count) {  
            ?instance a :Print ;
                :issuanceType :Monograph .
            ?hold :itemOf ?instance ;
                :heldBy lib:S .
            ?holdMeta :mainEntity ?hold ;
                :created ?date .

            BIND(month(?date) as ?month)
            FILTER(year(?date) = 2020)
        }
        GROUP BY ?month
        ORDER BY ?month

    **Kommentar:**
    Med katalogiserades menar vi här när beståndspost skapades. Svaret visar antal per månad.

---

* #### Hur många elektroniska seriella resurser katalogiserades av Kungliga biblioteket 2018?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT ?month (COUNT(DISTINCT ?instance) AS ?count) {  
            ?instance a :Electronic ;
                :issuanceType :Serial .
            ?hold :itemOf ?instance ;
                :heldBy lib:S .
            ?holdMeta :mainEntity ?hold ;
                :created ?date .

            BIND(month(?date) AS ?month)
            FILTER(year(?date) = 2018)
        }
        GROUP BY ?month
        ORDER BY ?month

---

* #### Hur många monografier inom DDK 320 katalogiserades av Umeå universitetsbibliotek 2019?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?instance) AS ?count) {  
            ?instance :issuanceType :Monograph ;
                :instanceOf/:classification [ a :ClassificationDdc ;
                        :code ?code ] .
            ?hold :itemOf ?instance ;
                :heldBy lib:Q .
            ?holdMeta :mainEntity ?hold ;
                :created ?date .

            FILTER(STRSTARTS(?code, "320"))
            FILTER(year(?date) = 2019)
        }

---

* #### Hur många poster katalogiserades med Svenska ämnesordet Missionärer 2010-2019?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?instance) AS ?count) {  
            ?instance :instanceOf/:subject sao:Mission%C3%A4rer ;
                ^:itemOf ?hold .
            ?holdMeta :mainEntity ?hold ;
                :created ?date .

            FILTER(year(?date) >= 2010 && year(?date) <= 2020)
        }

---

 * #### Hur många poster finns det inom bibliografin SUEC?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(?record) AS ?count) {
            ?record a :Record ;
                :bibliography lib:SUEC .
        }

---

 * #### Hur många nya personbeskrivningar (auktoritetsposter) med ISNI skapades 2017-2021?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?person) AS ?count) {
            ?meta :mainEntity ?person ;
                :created ?date .
            ?person a :Person ;
                :identifiedBy [ a :ISNI ]

            FILTER(year(?date) >= 2017 && year(?date) <= 2021)
        }

---

 * #### Hur många personbeskrivningar ändrades 2017-2021?

        PREFIX : <https://id.kb.se/vocab/>

        SELECT (COUNT(DISTINCT ?person) AS ?count) {
            ?meta :mainEntity ?person ;
                :modified ?date .
            ?person a :Person .

            FILTER(year(?date) >= 2017 && year(?date) <= 2021)
        }

    **Kommentar:**
    För att få motsvarande resultat för andra entitetstyper än personer räcker det att ändra `:Person` till önskad typ, t.ex. `:Organization`.

# Exempelfrågor SPARQL

----

Detta dokument innehåller ett antal exempelfrågor som demonstrerar hur vår [SPARQL endpoint](https://libris.kb.se/sparql/) 
kan användas för uttag av statistik från Libris XL. 
Vill man förstå syntaxen kan man med fördel läsa W3C:s [specifikation](https://www.w3.org/TR/sparql11-query/) av SPARQL. 

Datat är huvudsakligen uttryckt i [Bibframe](http://id.loc.gov/ontologies/bibframe/), med kompletterande termer
 från andra välkända vokabulär samt vår egen [KBV](https://id.kb.se/vocab/). Samtliga förekommande termer från
  externa vokabulär finns mappade till ekvivalenta termer i KBV. Termer som i KBV definierats som ekvivalenta är
   sinsemellan utbytbara, förutsatt att vi explicit anger att definitionerna i KBV ska appliceras när en fråga körs. 
   Detta gör vi med `define input:inference "kbv_rule_set"` (se exemplen).
   
   För enkelhetens skull är exempelfrågorna i största möjliga mån uttryckta med KBV-termer, som vi kan förkorta med 
    [namnrymdsprefixet](https://www.w3.org/TR/sparql11-query/#prefNames) `:`. Detta och andra vanliga prefix är 
    [fördefinierade](https://libris.kb.se/sparql/?help=nsdecl) i endpointen och behöver därför inte deklareras explicit
     i frågorna.

---

* #### Hur många romaner gavs ut under 2019?

        define input:inference "kbv_rule_set"
        define input:same-as "yes"
        
        SELECT (COUNT(DISTINCT ?novel) as ?count) {
            ?novel :instanceOf/:genreForm/(:exactMatch|^:exactMatch)* marc:Novel ;
                :publication/:year "2019" .
        }

   **Kommentar:**  
   Här frågar vi efter antalet _instanser_ vilket innebär att t.ex. olika bandtyper räknas individuellt. 
   Att räkna antalet unika verk är tyvärr inte möjligt i dagsläget, eftersom de flesta verk saknar URI:er att referera
    till.
   
   I nuvarande data identifierar vi romaner med genre/form-termen `marc:Novel`, men för att göra frågan beständig
    matchar vi även ekvivalenta termer. För ekvivalenta termer sammanlänkade med `owl:sameAs` kan vi ange
     `define input:same-as "yes"`; sådana länkar härleds då implicit. För att `exactMatch`-länkade termer
      till/från `marc:Novel` ska matchas och behöver det anges explicit i trippelmönstret.
   
   I denna fråga och många av de följande används s.k. [property paths](https://www.w3.org/TR/sparql11-query/#propertypaths) 
   för att inte behöva skriva ut varje trippel i sin helhet.
   
   KBV-termer kan som sagt bytas ut mot motsvarigheter i externa vokabulär. Nedan ser vi ett exempel på
    samma fråga uttryckt med andra termer.
   
        define input:inference "kbv_rule_set"
        define input:same-as "yes"
        
        SELECT (COUNT(DISTINCT ?novel) as ?count) {
            ?novel bf2:instanceOf/bf2:genreForm/(skos:exactMatch|^skos:exactMatch)* marcgt:nov ;
                :publication/:year "2019" .
        }
        
---

 * #### Vilka språk finns Selma Lagerlöf översatt till?
 
        define input:inference "kbv_rule_set"

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
    Här förutsätter vi att författaren alltid ligger som länkad entitet under `:agent`. En variant för att matcha även
     lokala entiteter vore att byta ut URI:n `<https://libris.kb.se/qn247n18248vs58#it>` mot en blanknod `[ :givenName
      "Selma" ; :familyName "Lagerlöf" ]`. Detta fungerar dock dåligt i det fall författaren har ett mer generiskt
       namn.  

 ---

 * #### Vilka språk har svensk utgivning översatts till mellan åren 2000-2010?
 
        define input:inference "kbv_rule_set"

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
    Vi kan nämligen ta reda på verkets originalspråk via `:translationOf`, däremot inget om dess
     originalutgivning.

 ---

  * #### Vilka svenska skönlitterära titlar har översatts till spanska 1990?
  
        define input:inference "kbv_rule_set"
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
            
            ?spanishInstance :publication/:year "1990" ;
                :instanceOf ?work .
            ?work :genreForm/(:exactMatch|^:exactMatch)* ?genre ;
                :language lge:spa ;
                :translationOf [ a :Work ;
                        :language lge:swe ] .
            OPTIONAL {
                ?spanishInstance :hasTitle [ a :Title ;
                        :mainTitle ?spanishTitle ] .
            }
            OPTIONAL {
                ?work :hasTitle [ a :Title ;
                        :mainTitle ?swedishTitle ] .
            }
        }     

     **Kommentar:**  
     Tyvärr är det inte möjligt att få fram vilka svenska verk som översatts till spanska då det kräver att verket
      ligger länkat under `:translationOf`. I dagsläget får vi nöja oss med spanska instanser som översatts _från_
       svenska.  
       Önskvärt vore såklart också att ha en superklass som representerar all skönlitteratur. Visserligen finns
        `https://id.kb.se/term/saogf/Skönlitteratur` men den används idag i mindre utsträckning och dess relation
         till marc-termerna är inte heller definierad.

  ---

  * #### Vilka serietecknare har översatts till svenska under 1980-2020?
        define input:inference "kbv_rule_set"
        define input:same-as "yes"

        SELECT DISTINCT ?cartoonist (CONCAT(?givenName, " ", ?familyName) as ?name) {
            VALUES ?genre {
                marc:ComicStrip
                marc:ComicOrGraphicNovel
            }
            
            [] :instanceOf [ :genreForm/(:exactMatch|^:exactMatch)* ?genre ;
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
    Serietecknare som ligger som lokala entiteter (blanknoder) under `:agent` filtreras här bort. Vill man ha med även
     de lokala entiteterna i resultatet tar man med fördel bort `FILTER(!isIri(?cartoonist))`, dock innebär detta att
      samma serietecknare kan förekomma flera gånger i resultatet.

----

* #### Hur många franska barnböcker översättes till svenska under 1980-2020?
        define input:inference "kbv_rule_set"
        define input:same-as "yes"

        SELECT (COUNT(DISTINCT ?book) AS ?count) {
            ?book :issuanceType kbv:Monograph ;
                :instanceOf [ a bf2:Text ;
                        :intendedAudience/(:exactMatch|^:exactMatch)* marc:Juvenile ;
                        :language lge:swe ;
                        :translationOf [ a bf2:Work ;
                                :language lge:fre ] ] ;
                :publication/:year ?year .
            
            FILTER(str(?year) >= "1980" && str(?year) < "2020")
        }    

    **Kommentar:**  
    Här frågar vi snarare efter antalet svenska resurser som översatts _från_ franska. Det omvända kräver att verken
    ligger länkade under `:translationOf`, vilket inte är fallet i dagsläget.  
    Vi frågar heller inte uteslutande efter böcker. Det är inte möjligt då det saknas en generell struktur som
     indikerar att en resurs är specifikt en bok. Däremot är det fullt möjligt att begränsa frågan till monografier
      (instansen) av typen text (verket).

 ---

 * #### Hur många böcker gavs ut på samiska utifrån aspekterna genre, målgrupp och utgivningsår?
        
        define input:inference "kbv_rule_set"
        
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
                :instanceOf [ a bf2:Text ;
                        :language ?language ;
                        :intendedAudience ?audience ;
                        :genreForm ?genre ] ;
                :publication/:year ?year .
            
            FILTER(!isIri(?genre))
        }
        GROUP BY ?year ?audience ?genre
        ORDER BY ?year ?audience ?genre

    **Kommentar:**  
    Det finns ingen URI som representerar alla samiska språk, utan vi får inkludera samtliga varieteter.

---

* #### Hur många facklitterära böcker gav förlaget Natur och Kultur ut mellan åren 1920-2000?
        
        define input:inference "kbv_rule_set"
        define input:same-as "yes"

        SELECT (COUNT(DISTINCT ?book) AS ?count) {
            ?book :issuanceType :Monograph ;
                :instanceOf [ a :Text ;
                        :genreForm/(:exactMatch|^:exactMatch)* marc:NotFictionNotFurtherSpecified ] ;
                :publication [ a :PrimaryPublication ;
                        :agent/:label ?agentLabel ;
                        :year ?year ] .
            
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
        
        define input:inference "kbv_rule_set"

        SELECT ?year (COUNT(DISTINCT ?book) AS ?count) {
            ?book bf2:issuance kbv:Monograph ;
                bf2:instanceOf/a bf2:Text ;
                kbv:publication [ a kbv:PrimaryPublication ;
                        kbv:year ?year ] ;
                ^bf2:itemOf/kbv:cataloguersNote "nbegenutg" .
        }
        ORDER BY ?year

---

* #### Hur många böcker har det getts ut inom barnlitteratur i Sverige varje år?

        define input:inference "kbv_rule_set"
        define input:same-as "yes"

        SELECT ?year (COUNT(DISTINCT ?book) AS ?count) {
            ?book :issuanceType :Monograph ;
                :instanceOf [ a :Text ;
                        :intendedAudience/(:exactMatch|^:exactMatch)* marc:Juvenile ] ;
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

        define input:inference "kbv_rule_set"

        SELECT ?year (COUNT(DISTINCT ?book) AS ?count) {
            ?book :issuanceType :Monograph ;
                :instanceOf/a :Text ;
                :publication [ a PrimaryPublication ;
                        :country ctry:sw ;
                        :year ?year ] .
        }
        GROUP BY ?year
        ORDER BY ?year

---

* #### Hur många böcker har digitaliserats under 2020?

        define input:inference "kbv_rule_set"

        SELECT (COUNT(DISTINCT ?digiBook) AS ?count) {
            ?digiBook :issuanceType :Monograph ;
                :instanceOf/a bf2:Text ;
                :production/(:date|:year) "2020" ;
                ^:mainEntity/:bibliography lib:DIGI .
        }
     
---

* #### Vilka titlar digitaliserades 2019?
        
        define input:inference "kbv_rule_set"

        SELECT DISTINCT ?digi ?title {
            ?digi :production/(:date|:year) "2019" ;
                :mainEntity/:bibliography lib:DIGI .
            OPTIONAL {
                ?digi :title [ a :Title ;
                        :mainTitle ?title ] .
            }
        }
        ORDER BY ?title
    
---

* #### Hur många svenska utgivare fanns det 1970?
        
        define input:inference "kbv_rule_set"

        SELECT (COUNT(DISTINCT ?publisherLabel) AS ?count) {
            [] a :PrimaryPublication ;
                :country ctry:sw ;
                :year "1970" ;
                :agent/:label ?publisherLabel .
        }

    **Kommentar:**  
    Här frågar vi snarare "Vilka utgivare gav ut något i Sverige under 1970?". Tillräckliga påståenden om utgivare för
     att besvara originalfrågan saknas i dagsläget. Utgivare ligger mestadels som lokala entiteter, där enbart dess
      benämningar anges. Här skulle vi istället vilja att utgivare tilldelats URI:er och länkats under `:agent`. 
      På respektive URI skulle sedan relevant information kunna samlas, exempelvis när förlaget grundats och landet
       det verkar i.
    
    Om utgivare var länkade skulle vi också få ett mer exakt resultat, tack vare att vi då skulle kunna garantera att
     antalet _unika_ utgivare räknas. Att räkna blanknoder fungerar inte eftersom vi inte kan särskilja vilka som
      representerar samma förlag. Istället räknar vi antalet unika benämningar, men inte heller detta sätt garanterar
       ett exakt resultat då det kan förekomma olika benämningar på samma förlag, t.ex. "Natur & Kultur" och "N&K". 

---

* #### Hur många barnböcker gavs ut på ett annat språk än svenska av svenska utgivare 2019?
        
        define input:inference "kbv_rule_set"
        define input:same-as "yes"

        SELECT (COUNT(DISTINCT ?book) AS ?count) {
            ?book :issuanceType kbv:Monograph ;
                :instanceOf [ a :Text ;
                        :intendedAudience/(:exactMatch|^:exactMatch)* marc:Juvenile ;
                        :language ?language ] ;
                :publication [ a :PrimaryPublication ;
                        :country ctry:sw ;
                        :year "2019" ] .
            
            FILTER(?language != lge:swe)
        }

---

* #### Vilka titlar har getts ut om coronapandemin 2019-2020 och coronaviruset?
        
        define input:inference "kbv_rule_set"

        SELECT DISTINCT ?instance ?title {
            VALUES ?subject {
                sao:Covid-19
                sao:Coronapandemin%202019-2020%20
                sao:Coronavirus
            }
            
            ?instance :instanceOf/:subject ?subject .
            OPTIONAL {
                ?instance :title [ a :Title ;
                        :mainTitle ?title ] .
            }
        }

---

* #### Hur många titlar har getts ut om coronapandemin 2019-2020 och coronaviruset?
        
        define input:inference "kbv_rule_set"

        SELECT (COUNT(DISTINCT ?instance) as ?count) {
            VALUES ?subject {
                sao:Covid-19
                sao:Coronapandemin%202019-2020%20
                sao:Coronavirus
            }
            
            ?instance :instanceOf/:subject ?subject
        }

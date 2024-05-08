import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap

String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

PrintWriter IDreport = getReportWriter("ID-report-${currentDate}.csv")
PrintWriter manCheck = getReportWriter("Manuell-kontroll-${currentDate}.csv")
PrintWriter wrongInput = getReportWriter("Wrong-input-not-run-${currentDate}.csv")
PrintWriter failedHoldIDs = getReportWriter("Failed-holdIDs-${currentDate}.txt")
PrintWriter dup = getReportWriter("Duplicate-input-${currentDate}.txt")

// Läs in datum för senaste körning
File lastRunFile = new File(System.getProperty("last-run-file", new File(scriptDir, "lastRun.Date").toString()))
def lastRunDate = lastRunFile.exists() ? lastRunFile.readLines().first() : ""
println("Using last run timestamp: $lastRunDate")
url = "https://inv.kb.se/nb/export.php?timestamp=$lastRunDate" // url för att hämta data från applikationen

inputRows = new URL(url).text
List<String> ProgramLines = inputRows.readLines()

def itemList = Collections.synchronizedList([])

IDreport.println("Inventarienummer;Titel;Input ISBN;Matching bib;identifiedBy;indirectlyIdentifiedBy;Nr of holdings;Sigel S Holding;New holding record;Comments;Save copy?")
manCheck.println("Inventarienummer;Input ISBN;Matching bib;identifiedBy;indirectlyIdentifiedBy;Nr of holdings;Sigel S Holding;New holding record;Comments;Save copy?")

for (String operation : ProgramLines) {
    if (operation.isEmpty()) { // skippa tomma rader
        continue
    }
    if (!(operation =~ /(\d*)(\t)(saknas)/)) { // Skippa rad om barcode saknas
    String[] part = operation.split('\t')
    String invnr = part[0].trim().replace(' ','').replace('.','') // strippa mellanslag och punkt i Inventarienummer
    String fuzzyID = part[1].trim().toUpperCase().replace(' ','').replace('-','') // strippa mellanslag och streck från ISBN
 
    if (!(invnr ==~ /^(\d{8})\b/)) { // om Inventarienummer (första kolumnen) inte är åtta siffror, logga och gå vidare
        wrongInput.println("$invnr;$fuzzyID;Incorrect input - Not run against the database;")
        continue
    }
    
    String lopnr = invnr.replaceFirst('^0+(?!$)', '') // Strippa inledande nollor i Inventarienummer
    
    where = """id in (
            select id from lddb where
            collection='bib' and
            (data#>'{@graph,1,identifiedBy}' @> '[{\"@type\":\"ISBN\", \"value\":\"$fuzzyID\"}]'
            or
            (data#>'{@graph,1,indirectlyIdentifiedBy}' @> '[{\"@type\":\"ISBN\", \"value\":\"$fuzzyID\"}]'
            and data#>>'{@graph,1,@type}' IN ('Instance','Print','TextInstance')))
            )"""

    Set bibIds = ConcurrentHashMap.newKeySet()

    // Hitta matchande bib-poster
    selectBySqlWhere(where, { bib ->
        bibIds << bib.doc.getShortId()
    })

    if (bibIds.isEmpty()) {
        // Om inga bib-poster kunde hittas, logga och hoppa vidare till nästa rad i for-loopen
        IDreport.println("$lopnr;;$fuzzyID;NO MATCHING RECORD;;;;;;")
            manCheck.println("$lopnr;$fuzzyID;NO MATCHING RECORD;;;;;;Not found in Libris")
        continue
    }

    if (bibIds.size() > 1) {
        // Om flera bib-poster matchar, logga och hoppa sen vidare till nästa rad i for-loopen
        IDreport.println("$lopnr;;$fuzzyID;MULTIPLE HITS: $bibIds;;;;;;")
            manCheck.println("$lopnr;$fuzzyID;MULTIPLE HITS: $bibIds;;;;;;More than one bib found")
        continue
    }
    
    // Om exakt en bib-post matchar kan vi gå fortsätta med denna
    selectByIds(bibIds, {bib ->
        def bibMainEntity = bib.graph[1]["@id"]
        def title = getAtPath(bib.graph, [1, "hasTitle", 0, "maintitle"], "<saknas>")
        List ISBN = []
        List IISBN = []
        
        bib.graph[1]["identifiedBy"].each { // Sök och spara value från identifiedBy med ISBN och där värdet inte är null
                if (it["@type"] == "ISBN" && it["value"] != null) {
                ISBN.add(it.value.trim())
                }
            }

        bib.graph[1]["indirectlyIdentifiedBy"].each { // Sök och spara value från indirectlyIdentifiedBy med ISBN och där värdet inte är null
                if (it["@type"] == "ISBN" && it["value"] != null) {
                IISBN.add(it.value.trim())
                }
            }

        boolean foundHold = false
        Set holdIds = ConcurrentHashMap.newKeySet()
        def HC = 0

        selectBySqlWhere("""
                data#>>'{@graph,1,itemOf,@id}' = '${bibMainEntity}' AND
                collection = 'hold' AND
                deleted = 'false'
        """, silent: true) { hold ->   
        
                holdIds << hold.doc.getShortId()
                HC = holdIds.size() // räkna antalet holds
        }

        
    selectByIds(holdIds, {hold ->
 
            if (hold.doc.getHeldBySigel() == "S" && !ISBN) {
                foundHold = true
                IDreport.println("$lopnr;$title;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;${hold.doc.shortId};;No ISBN in identifiedBy")           
                    manCheck.println("$lopnr;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;${hold.doc.shortId};;No ISBN in identifiedBy")
            }
 
            if (hold.doc.getHeldBySigel() == "S" && ISBN) {
                foundHold = true
                boolean HT = false
                
                String holdText = hold.graph[1] // spara hold grafen och kontrollera sedan mot ett antal textsträngar
                                if (holdText.toLowerCase().contains("defekt") || holdText.toLowerCase().contains("reklameras")
                                    || holdText.toLowerCase() =~ /fackex.*?ej ink/ || holdText.toLowerCase() =~ /referensex.*?ej ink/
                                    || holdText.toLowerCase().contains("desiderata") || holdText.toLowerCase().contains("leveransbevakas")
                                    || holdText.toLowerCase().contains("förkommen") || holdText.toLowerCase().contains("saknas")
                                    || holdText.toLowerCase().contains("berghman") || holdText.toLowerCase().contains("refkb")) {
                HT = true
                }
                
                if (HT) { // Boken kan saknas i KB:s pliktsvit och ska därför manuellt kontrolleras
                    
                        IDreport.println("$lopnr;$title;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;${hold.doc.shortId};;S holding found;Boken saknas ev i pliktsviten KB. Kontrollera!")
                            if (ISBN.size() > 1) {
                                manCheck.println("$lopnr;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;${hold.doc.shortId};;Multiple ISBN;Boken saknas ev i pliktsviten KB. Kontrollera!")    
                        }
                }
                else{
                        IDreport.println("$lopnr;$title;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;${hold.doc.shortId};;S holding found")
                            if (ISBN.size() > 1) {
                                manCheck.println("$lopnr;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;${hold.doc.shortId};;Multiple ISBN")    
                        }
                }
            }
            })       

        if (!foundHold && !ISBN) { // Om det saknas S-bestånd och inget ISBN i identifiedBy (ingen entydig träff), logga endast
            IDreport.println("$lopnr;$title;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;;;No ISBN in identifiedBy")
                manCheck.println("$lopnr;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;;;No ISBN in identifiedBy")
        }    

        if (!foundHold && ISBN) { // Om S-bestånd saknas och ISBN finns i identifiedBy - skapa ny beståndspost
            String iNr = "INV2021-$lopnr" // Skapa ny sträng för att inte få GString problem i holdData
            def holdData =
                    [ "@graph": [
                            [
                                    "@id": "TEMPID",
                                    "@type": "Record",
                                    "mainEntity" : ["@id": "TEMPID#it"]
                            ],
                            [
                                    "@id": "TEMPID#it",
                                    "@type": "Item",
                                    "heldBy": ["@id": "https://libris.kb.se/library/S"],
                                    "itemOf": ["@id": bibMainEntity],
                                    "hasComponent": [
                                            [
                                                    "@type": "Item",
                                                    "availability": ["@id": "https://id.kb.se/term/enum/Delivered"],
                                                    "hasNote": [
                                                    "@type": "Note",
                                                    "label": "Inkommen för katalogisering"
                                                    ],
                                                    "shelfControlNumber": iNr,
                                                    "cataloguersNote": ["Automatiskt skapad beståndspost, inventeringsprojektet 2021 (INV2021)"],
                                                    "heldBy": ["@id": "https://libris.kb.se/library/S"]
                                            ]
                                    ]
                            ]
                    ]]

            def item = create(holdData)
            itemList.add(item)

                IDreport.println("$lopnr;$title;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;;${item.doc.shortId};New holding created")
                    if (ISBN.size() > 1) {
                        manCheck.println("$lopnr;$fuzzyID;${bib.doc.shortId};$ISBN;$IISBN;$HC;;${item.doc.shortId};Multiple ISBN")   
                    }      
            
        }
    })
}
}

selectFromIterable(itemList, { newlyCreatedItem ->
    String newInvNr = newlyCreatedItem.graph[1].hasComponent[0].shelfControlNumber // Hämta Inventarienummer från det nya beståndet
    newlyCreatedItem.scheduleSave(onError: { e ->
        failedHoldIDs.println("Failed to save ${newlyCreatedItem.doc.shortId} due to: $e")
        dup.println("$newInvNr is a duplicate from the input-list") // Logga dubbletter i inputlistan i egen rapport
    })
})

lastRunFile.withWriter { writer -> 
    writer.write(currentDate) // spara dagens datum i lastRun.Date
}

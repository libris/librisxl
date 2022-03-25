PrintWriter failedHoldIDs = getReportWriter("Failed-to-create-holds.txt")
PrintWriter failedModified = getReportWriter("Failed-to-modify-bibs.txt")

File input = new File(scriptDir, "littbanken_hold_link.csv")
List<String> ProgramLines = input.readLines().drop(1)

def itemList = []

for (String operation : ProgramLines) {
    
    String[] part = operation.split(';', -1)
    String librisCN = part[0].trim()
    String librisID = part[1].trim()
    String status = part[2].trim() // not used
    String url = part[3].trim()
    String epub = part[4].trim()
    String omslagsbild = part[5].replace("(större)","").trim()

    String shortUrl = url.replaceFirst("https://","").trim()
    String shortEpub = epub.replaceFirst("https://","").trim()
	String normUrl = shortUrl.replaceAll("å","a").replaceAll("ä","a").replaceAll("ö","o")
	String normEpub = shortEpub.replaceAll("å","a").replaceAll("ä","a").replaceAll("ö","o")
    String shortCover = omslagsbild.replaceFirst("https://","").trim()

    if (librisCN) {

    where = """id in
        (select lb.id
        from lddb lb
        where lb.collection = 'bib' and
        lb.data#>>'{@graph,0,controlNumber}' = '${librisCN}'
        and data#>>'{@graph,1,@type}' IN ('Electronic')
    )"""
    }
    else {
    where = """id = '${librisID}'"""
    }

    selectBySqlWhere(where, { bib ->
        def bibMainEntity = bib.graph[1]["@id"]
        def instance = bib.graph[1]
        boolean foundUsage = false
        boolean foundUrl = false
        boolean foundEpub = false
        boolean foundCover = false
        
        if (!instance["usageAndAccessPolicy"]) { // lägg till usageAndAccessPolicy om det inte finns
            instance["usageAndAccessPolicy"] = []
        }
        
        if (instance["usageAndAccessPolicy"]) {

            instance["usageAndAccessPolicy"].each { it ->
                asList(it["uri"]).each { uri -> 
                    if (uri instanceof String && uri.contains("litteraturbanken.se/om/rattigheter")) { // kolla om uri matchar Litteraturbankens url om rättigheter
                    foundUsage = true
                	}	
                }
            }
        }
        
        if (!instance["associatedMedia"]) { // lägg till associatedMedia om det inte finns
            instance["associatedMedia"] = []
        }
        
        if (instance["associatedMedia"]) {

            instance["associatedMedia"].each { it ->
                asList(it["uri"]).each { uri ->
                    if (uri instanceof String && uri.contains("${shortUrl}")) { // kolla om uri matchar url från infilen (https:// borttaget då det kan vara sparat med http i Libris och vi vill inte skapa dubbletter)
                    foundUrl = true
                	}
                    if (uri instanceof String && uri.contains("${normUrl}")) { // uri är ofta utan å,ä,ö - därför kollas också en sådan "normaliserad" version
                    foundUrl = true
                    }
	
                    if (epub) {
                        if (uri instanceof String && uri.contains("${shortEpub}")) { // gör motsvarande för epub
                            foundEpub = true
                        }
                        if (uri instanceof String && uri.contains("${normEpub}")) { 
                            foundEpub = true
                        }
                    }
                }
            }
        }

        if (omslagsbild && !instance["isPrimaryTopicOf"]) { // lägg till isPrimaryTopicOf om det inte finns
            instance["isPrimaryTopicOf"] = []
        }
        
        if (instance["isPrimaryTopicOf"]) {

            instance["isPrimaryTopicOf"].each { it ->
                asList(it["uri"]).each { uri ->
 	
                    if (omslagsbild) {
                        if (uri instanceof String && uri.contains("${shortCover}")) { // kolla om uri matchar url för omslag från infilen, https:// borttaget
                            foundCover = true
                        }
                    }
                }
            }
        }
        
        if (!foundUsage) {
            instance["usageAndAccessPolicy"] <<
            [
             "@type": "UsePolicy",
             "label": "Rättigheter för Litteraturbankens texter",
             "uri": ["http://litteraturbanken.se/om/rattigheter"]
             ]
        }
        
        if (!foundUrl) {
            instance["associatedMedia"] <<
            [
            "uri": ["${url}"],
            "@type": "MediaObject",
            "cataloguersNote": ["856free"],
            "marc:publicNote": "Fritt tillgänglig via Litteraturbankens webbplats"
            ]
        }
        
          if (epub && !foundEpub) {
            instance["associatedMedia"] <<
            [
            "uri": ["${epub}"],
            "@type": "MediaObject",
            "marc:publicNote": "Hämta gratis epub direkt från Litteraturbanken"
            ]
        }
        
        if (omslagsbild && !foundCover) {
            instance["isPrimaryTopicOf"] <<
            [
            "uri": ["${omslagsbild}"],
            "@type": "Document",
            "cataloguersNote": ["digipic"],
            "marc:publicNote": "Titelsida"
            ]
        }
        
        bib.scheduleSave(loud: true, onError: { e ->
            failedModified.println("Failed to update ${bib.doc.shortId} due to: $e")
        })
        
        // kolla om hold finns
        
        boolean foundHold = false

        selectBySqlWhere("""
                data#>>'{@graph,1,itemOf,@id}' = '${bibMainEntity}' AND
                collection = 'hold' AND
                data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/LITT' AND
                deleted = 'false'
        """, silent: true) { hold ->   
        
        foundHold = true
        }
        
         if (!foundHold) { // Om LITT-bestånd saknas, skapa ny (minimal) beståndspost
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
                                    "heldBy": ["@id": "https://libris.kb.se/library/LITT"],
                                    "itemOf": ["@id": bibMainEntity],
                                    "hasComponent": [
                                            [
                                                    "@type": "Item",
                                                    "heldBy": ["@id": "https://libris.kb.se/library/LITT"]
                                            ]
                                    ]
                            ]
                    ]]

            def item = create(holdData)
            itemList.add(item)
         }
    })
}

private List asList(Object o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
} 

selectFromIterable(itemList, { newlyCreatedItem ->
    newlyCreatedItem.scheduleSave(onError: { e ->
        failedHoldIDs.println("Failed to save ${newlyCreatedItem.doc.shortId} due to: $e")
    })
})



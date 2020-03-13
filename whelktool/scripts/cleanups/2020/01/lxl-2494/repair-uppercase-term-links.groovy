/*
 * * See LXL-2494 for more info.
 */

scheduledForChange = getReportWriter("scheduled-for-change")

where = "collection = 'auth' AND data#>>'{@graph,1,inCollection}' is not null"

links = ["https://id.kb.se/term/Fack" : "https://id.kb.se/term/fack",
         "https://id.kb.se/term/Skon" : "https://id.kb.se/term/skon",
         "https://id.kb.se/term/Musik": "https://id.kb.se/term/musik",
         "https://id.kb.se/term/NLT"  : "https://id.kb.se/term/nlt"
]

selectBySqlWhere(where, silent: false) { docItem ->
    List<Map> inCollection = docItem.doc.data["@graph"][1]["inCollection"]
    
    links.each { invalidLink, validLink ->
        for (term in inCollection) {
            if (term.'@id' == invalidLink) {
                term.'@id' = validLink
                docItem.scheduleSave()
                scheduledForChange.println "Scheduling save for: ${docItem.doc.getURI()}"
            }
        }
    }
}

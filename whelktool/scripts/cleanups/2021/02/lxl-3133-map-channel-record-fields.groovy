List channelRecs = Collections.synchronizedList([])

selectBySqlWhere("collection = 'bib' AND data#>>'{@graph,1,marc:mediaTerm}' = 'channel record'") { cRec ->
    Map cRecData = [:]

    cRecData["id"] = cRec.graph[1]["@id"]
    cRecData["controlNumber"] = cRec.graph[0]["controlNumber"]
    cRecData["655marcgt"] = cRec.graph[1].instanceOf?.genreForm.findAll {
        it.inScheme?."@id" == "https://id.kb.se/term/marcgt"
    }
    cRecData["856website"] = cRec.graph[1]["isPrimaryTopicOf"].find {
        it["marc:publicNote"] == "web site"
    }

    channelRecs << cRecData
}

selectBySqlWhere("collection = 'bib' AND data#>'{@graph,1,supplementTo}' IS NOT NULL") { data ->

    List supplementTo = data.graph[1]["supplementTo"]

    Map channelRec = channelRecs.find { cr ->
        supplementTo.any { st ->
            st["@id"] == cr["id"] || st.describedBy?.any { it["controlNumber"] == cr["controlNumber"] }
        }
    }

    if (!channelRec)
        return

    boolean modified

    List fields655 = data.graph[1].instanceOf?.genreForm
    List fields856 = data.graph[1]["isPrimaryTopicOf"]

    // Add "web site" field from channel rec if missing
    if (channelRec["856website"]) {
        if (fields856 == null) {
            data.graph[1]["isPrimaryTopicOf"] = [channelRec["856website"]]
            modified = true
        } else if (!(channelRec["856website"] in fields856)) {
            data.graph[1]["isPrimaryTopicOf"] << channelRec["856website"]
            modified = true
        }
    }

    // Remove field with "marcgt" and "article" if not in channel rec
    if (channelRec["655marcgt"].isEmpty()) {
        modified |= fields655?.removeAll { it.inScheme?."@id" == "https://id.kb.se/term/marcgt" && it["prefLabel"] == "article" }
        if (fields655?.isEmpty())
            data.graph[1]["instanceOf"].remove("genreForm")
    }
    // Add field with "marcgt" from channel rec if missing
    else if (fields655 == null) {
        data.graph[1]["instanceOf"]["genreForm"] = channelRec["655marcgt"]
        modified = true
    }
    // Replace/add fields with "marcgt" from channel rec if different/missing
    else if (channelRec["655marcgt"].any{ !(it in fields655) }) {
        fields655.removeAll { it.inScheme?."@id" == "https://id.kb.se/term/marcgt" }
        fields655 += channelRec["655marcgt"]
        data.graph[1]["instanceOf"]["genreForm"] = fields655
        modified = true
    }

    if (modified)
        data.scheduleSave()
}
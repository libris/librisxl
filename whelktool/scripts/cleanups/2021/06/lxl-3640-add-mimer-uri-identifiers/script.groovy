List idList = new File(scriptDir, 'idlist.yaml').readLines()

Map mimerUriById = [:]

String id

idList.each { String line ->
    if (line =~ "libris-id:")
        id = line.replaceFirst(/.*libris-id:/, "").trim()
    else if (line =~ /uri:/) {
        String uri = line.replace("uri:", "").trim()

        if (id.isNumber()) {
            String where = """
                collection = 'bib' 
                AND deleted = 'false' 
                AND data#>>'{@graph,0,controlNumber}' = '${id}'
            """

            selectBySqlWhere(where) {
                id = it.doc.shortId
            }
        }

        mimerUriById[id] = uri
    }
}

selectByIds(mimerUriById.keySet()) { data ->
    Map instance = data.graph[1]
    List identifiedBy = instance.identifiedBy

    Map identifier =
            [
                "@type": "URI",
                "value": mimerUriById[data.doc.shortId]
            ]

    if (identifiedBy) {
        if (!(identifier in identifiedBy)) {
            identifiedBy << identifier
            data.scheduleSave()
        }
    }
    else {
        instance["identifiedBy"] = [identifier]
        data.scheduleSave()
    }
}








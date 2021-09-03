List suecia = new File(scriptDir, 'suecia-group-idlist.yaml').readLines()
List f1700 = new File(scriptDir, 'f1700-group-idlist.yaml').readLines()

Map mimerUriById = [:]

String id

(suecia + f1700).each { String line ->
    if (line =~ "libris_id:")
        id = line.replaceFirst(/.*libris_id:/, "").replaceAll(/\W/, "")
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








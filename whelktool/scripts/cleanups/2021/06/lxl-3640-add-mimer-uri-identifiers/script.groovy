Map mimerIdByLibrisId = [:]

new File(scriptDir + '/id-lists').eachFile {f ->
    String librisId

    f.eachLine { String line ->
        if (line =~ "libris_id:")
            librisId = line.replaceFirst(/.*libris_id:/, "").replaceAll(/\W/, "")
        else if (line =~ /local:/) {
            String local = line.replace("local:", "").trim()

            if (librisId.isNumber()) {
                String where = """
                collection = 'bib'
                AND deleted = 'false'
                AND data#>>'{@graph,0,controlNumber}' = '${librisId}'
            """

                selectBySqlWhere(where) {
                    librisId = it.doc.shortId
                }
            }

            mimerIdByLibrisId[librisId] = local
        }
    }
}

selectByIds(mimerIdByLibrisId.keySet()) { data ->
    Map instance = data.graph[1]
    List identifiedBy = instance.identifiedBy

    Map identifier =
            [
                "@type": "Identifier",
                "value": mimerIdByLibrisId[data.doc.shortId]
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







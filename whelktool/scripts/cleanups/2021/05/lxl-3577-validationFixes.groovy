import whelk.util.DocumentUtil

String where = "collection = 'auth'"

selectBySqlWhere(where) { data ->
    boolean changed = false

    def (record, mainEntity) = data.graph
    changed |= clearEmptyNationality(mainEntity)
    changed |= clearRecordMarcGarbage(record)
    changed |= clearGarbageProperties(data.graph)
    changed |= clearRecordDescConvNZ(record)

    if (changed) {
        pruneEmptyStructure(data.graph)
        data.scheduleSave()
    }
}

boolean clearEmptyNationality(Map mainEntity) {
    Object nationality = mainEntity["nationality"]
    if (nationality != null && nationality instanceof List && nationality.size() == 1 && nationality[0].isEmpty()) {
        mainEntity.remove("nationality")
        return true
    }
    return false
}

boolean clearRecordMarcGarbage(Map record) {
    boolean changed = false
    for (String key : ["marc:typeOfSeries", "marc:numberedSeries", "marc:headingSeries"]) {
        if (record[key] != null) {
            record.remove(key)
            changed = true
        }
    }
    return changed
}

boolean clearGarbageProperties(List graph) {
    boolean changed = DocumentUtil.traverse(graph) { value, List path ->
        if (!path) {
            return
        }
        def key = path.last() as String
        if (["marc:languageNote", "generic:b", "generic:a", "generic:", "generic:g%20V%C3%A4xter"].contains(key)) {
            return new DocumentUtil.Remove()
        }
    }
    return changed
}

boolean clearRecordDescConvNZ(Map record) {
    Object descConv = record["descriptionConventions"]
    if (descConv != null) {
        if (descConv instanceof List) {
            Iterator it = descConv.iterator()
            while (it.hasNext()) {
                Map conv = it.next()
                if (conv["code"] != null && (conv["code"] == "z" || conv["code"] == "n"))
                    it.remove()
            }
        } else if (descConv instanceof Map) {
            if (descConv["code"] != null && (descConv["code"] == "z" || descConv["code"] == "n")) {
                record.remove("descriptionConventions")
            }
        }
    }
}

void pruneEmptyStructure(Object from) {
    if (from instanceof List) {
        Iterator it = from.iterator()
        while (it.hasNext()) {
            Object o = it.next()
            if (o != null) {
                pruneEmptyStructure(o)
                if (o.isEmpty()) {
                    it.remove()
                }
            }
        }
    } else if (from instanceof Map) {
        List<String> toBeRemoved = []
        for (Object key : from.keySet()) {
            if (from[key] != null) {
                pruneEmptyStructure(from[key])
                if (from[key].isEmpty())
                    toBeRemoved.add(key)
            }
        }
        if (!toBeRemoved.isEmpty()) {
            for (Object key : toBeRemoved)
                from.remove(key)
        }
    }
}

/**
 *
 * See LXL-3187 for more information
 */

report = getReportWriter("report.txt")
gmgpcPrefixSwe = "https://id.kb.se/term/gmgpc/swe/"
saogfPrefix = "https://id.kb.se/term/saogf/"

selectByCollection('auth') { data ->
    def instance = data.graph[1]

    if (!instance?.inScheme?.'@id'?.contains("https://id.kb.se/term/gmgpc")) {
        return
    }

    def relations = ['broader', 'narrower', 'related']

    boolean updated = false

    relations.each { relation ->
        if (!instance[relation]) {
            return
        }

        //Objects containing @type, prefLabel and sameAs
        def toRemove = instance[relation].findAll{ hasSaogfSameAs(it) }
        def toAdd  = toRemove.collect{ ['@id' : gmgpcPrefixSwe + it.prefLabel] }

        //Objects containing a single id key-value pair
        def toRemoveSimple = instance[relation].findAll{ it[ID]?.contains(saogfPrefix)}
        def toAddSimple = toRemoveSimple.collect { ['@id' : gmgpcPrefixSwe + it[ID].tokenize('/').last()] }

        instance[relation].removeAll{ hasSaogfSameAs(it) }
        instance[relation].removeAll{ it[ID]?.contains(saogfPrefix) }
        instance[relation].addAll(toAdd + toAddSimple)

        if (toRemove || toRemoveSimple) {
            updated = true
        }
    }

    //Remove sameAs-part from closeMatch for international gmgpc
    instance.closeMatch?.each { closeMatch ->
        if (!closeMatch.inScheme?.'@id'?.contains("swe") && hasSaogfSameAs(closeMatch)) {
            closeMatch.remove("sameAs")
            updated = true
        }
    }

    if (updated) {
        data.scheduleSave()
        report.println "${data.graph[0][ID]}, ${instance[ID]}"
    }
}

private boolean hasSaogfSameAs(relation) {
        return relation.sameAs?.any { sameAs -> sameAs.'@id'.contains(saogfPrefix) }
}
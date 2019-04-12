/*
 * Count frequency of inScheme codes that are not defined
 * in definitions/source/schemes.ttl
 *
 * See LXL-2437 for more info.
 *
 */

INSCHEME = 'inScheme'
PROPERTIES = ['subject', 'genreForm', 'classification']

SCHEMES = [:]
POTENTIAL_SCHEMES = [:]
INSCHEMES_WITH_LIST = [] as Set
PrintWriter potentialInSchemes = getReportWriter("potential-InSchemes.csv")
PrintWriter errorInSchemeAsList = getReportWriter("error-inScheme-as-list")


selectBySqlWhere("""
        data#>>'{@graph,1,@type}' in ('ConceptScheme') 
        AND collection = 'definitions'
    """) { data ->

    def (record, thing) = data.graph

    if (!thing) return

    SCHEMES << [(thing.code):thing[ID]]
}

selectBySqlWhere("""
        data#>>'{@graph,2}' LIKE '%${INSCHEME}%' AND collection in ('bib', 'hold')
    """) { data ->

    def (record, instance, work) = data.graph

    if (!work) return

    PROPERTIES.each {it ->
        checkInScheme(work[it], data)
    }
}

POTENTIAL_SCHEMES.each { k, v ->
    potentialInSchemes.println("${k},${v[0]},${v.tail()}")
}

INSCHEMES_WITH_LIST.each {
    errorInSchemeAsList.println("${it}")
}


void checkInScheme(obj, data) {
    obj.each {

        if (!it[INSCHEME]) {
            return
        }

        // inScheme should not be a list but still there exists some
        if (it[INSCHEME] instanceof List) {
            // Log all occurrences
            INSCHEMES_WITH_LIST << data.graph[0][ID]

            if (it[INSCHEME].size() == 1) {
                if (it[INSCHEME][0][ID])
                    return
                if (it[INSCHEME][0].code)
                    checkLinkFindability(it[INSCHEME][0].code, data)
            }
        } else {
            if (it[INSCHEME][ID])
                return

            if (it[INSCHEME].code) {
                checkLinkFindability(it[INSCHEME].code, data)
            }
        }
    }
}

void checkLinkFindability(localCode, data) {
    def code = localCode

    if (code instanceof List)
        code = code.join()

    code = code.replaceAll(" ", "").toLowerCase()

    if (!SCHEMES[code]) {
        if (!POTENTIAL_SCHEMES[localCode]) {
            List list = [1]
            list << data.graph[0][ID]
            POTENTIAL_SCHEMES << [(localCode):list]
        }
        else {
            POTENTIAL_SCHEMES[localCode][0] += 1
            if (POTENTIAL_SCHEMES[localCode].size() < 5) {
                POTENTIAL_SCHEMES[localCode] << data.graph[0][ID]
            }

        }
    }
}

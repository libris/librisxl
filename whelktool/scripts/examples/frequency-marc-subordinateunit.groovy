/*
 *
 * This script:
 * - Counts frequency of marc:subordinateUnit
 * - Counts frequency of linkable values in marc:subordinateUnit and isPartOf
 *
 * See LXL-2557 for more info.
 *
 */

TERM_TO_REMODEL = "marc:subordinateUnit"
TYPES_TO_REMODEL = ["Organization", "Jurisdiction", "Meeting"]

FREQUENCY = [:]
LINKABLE = [:]
NOT_LINKABLE = [:]
PrintWriter frequency = getReportWriter("frequency-bib.csv")
PrintWriter linkable = getReportWriter("linkable-bib.csv")
PrintWriter notLinkable = getReportWriter("not-linkable-bib.csv")

selectBySqlWhere('''
        data::text LIKE '%"marc:subordinateUnit"%' AND collection = 'bib'
        ''') { data ->

    // Skipping record
    data.graph[1..-1].each {
        findSubordinatedUnitInData(data, it)
    }
}

FREQUENCY.sort().each { k, v ->
    frequency.println("${k},${v[0]},${v.tail()}")
}

LINKABLE.sort().each { k, v ->
    linkable.println("${k},${v[0]},${v.tail()}")
}

NOT_LINKABLE.sort().each { k, v ->
    notLinkable.println("${k},${v}")
}

void checkFrequency(data, value, type) {

    int number
    if (value instanceof List)
        number = value.size()
    else
        number = 1

    //auth
    //key = "${data.graph[1][TYPE]}_${type}_${number}"

    key = "${type}_${number}"
    if (!FREQUENCY[key]) {
        List list = [1]
        list << data.graph[0][ID]
        FREQUENCY[key] = list
    }
    else {
        FREQUENCY[key][0] += 1
        if (FREQUENCY[key].size() < 1001) {
            FREQUENCY[key] << data.graph[0][ID]
        }
    }
}

void checkLinkabilitySubUnit(data, value) {
    value.flatten().each {
        checkLinkability(data, it)
    }
}

void checkLinkability(data, value) {

    if (value == null || value.isEmpty())
        return

    def name = value.replaceAll("\\.", "").replaceAll("'", "").toLowerCase()

    if (NOT_LINKABLE.containsKey(name)) {
        NOT_LINKABLE[name] += 1
    }
    else if (LINKABLE.containsKey(name)) {
        LINKABLE[name][0] += 1
        if (LINKABLE[name].size() < 11) {
            LINKABLE[name] << data.graph[0][ID]
        }
    } else {
        if (getUri(name)) {
            List list = [1]
            list << data.graph[0][ID]
            LINKABLE[name] = list
        }
        else {
            NOT_LINKABLE[name] = 1
        }
    }
}

String getUri(term) {
    String uri
    selectBySqlWhere("""
            collection = 'auth' 
            AND (lower(data#>>'{@graph,1,name}') = '${term}' OR lower(data#>>'{@graph,1,prefLabel}') = '${term}') 
            AND lddb.deleted = FALSE
    """) { doc ->
        uri = doc.graph[1][ID]
    }
    return uri
}


void findSubordinatedUnitInData(data, obj) {
    if (obj instanceof List) {
        obj.each {
            findSubordinatedUnitInData(data, it)
        }
    } else if (obj instanceof Map) {
        def type = obj[TYPE]
        obj.each { key, value ->
            if (key == TERM_TO_REMODEL && TYPES_TO_REMODEL.contains(type)) {
                List subordinateUnit = (value instanceof List) ? value : [value]
                checkFrequency(data, subordinateUnit, type)
                checkLinkabilitySubUnit(data, subordinateUnit)
            }
            if (key == 'isPartOf') {
                if (value.name instanceof List) {
                    checkLinkabilitySubUnit(data, value.name)
                }
                else {
                    checkLinkability(data, value.name)
                }
            }
            findSubordinatedUnitInData(data, value)
        }
    }
}




/*
 *
 * This script remodels marc:subordinateUnit
 * of types Organization, Jurisdiction and Meeting
 *
 * See LXL-2557 for more info.
 *
 */

TERM_TO_REMODEL = "marc:subordinateUnit"
TYPES_TO_REMODEL = ["Organization", "Jurisdiction", "Meeting"]

void remodelObjects(data, value, type) {

    if (TYPES_TO_REMODEL[2] == type) {
        //Meeting
        return
    } else {
        //Organization and Jurisdiction
        return
    }
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
                remodelObjects(data, value, type)
            }
            findSubordinatedUnitInData(data, value)
        }
    }
}

selectBySqlWhere('''
        data::text LIKE '%"marc:subordinateUnit"%' AND collection = 'bib'
        ''') { data ->

    // Skipping record
    data.graph[1..-1].each {
        findSubordinatedUnitInData(data, it)
    }
}

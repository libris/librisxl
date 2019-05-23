
COUNTER = [:]
PrintWriter FREQUENCY_REPEATED_SUBORDINATEUNIT = getReportWriter("frequency-repeated-subordinateunit-bib.csv")

void countAndLogObjects(data, value) {
    if (!COUNTER[value.size()]) {
        List example_records = [1]
        example_records << data.graph[0][ID]
        COUNTER << [(value.size()):example_records]
    }
    else {
        COUNTER[value.size()][0] += 1
        if (COUNTER[value.size()].size() < 10) {
            COUNTER[value.size()] << data.graph[0][ID]
        }
    }
}

void findSubordinatedUnitInData(data, obj) {
    if (obj instanceof List) {
        obj.each {
            findSubordinatedUnitInData(data, it)
        }
    } else if (obj instanceof Map) {
        obj.each { key, value ->
            if (key == "marc:subordinateUnit") {
                countAndLogObjects(data, value)
            }
            findSubordinatedUnitInData(data, value)
        }
    }
}

selectBySqlWhere('''
        data::text LIKE '%"marc:subordinateUnit"%' AND collection = 'auth'
        ''') { data ->

    // Skipping record
    data.graph[1..-1].each {
        findSubordinatedUnitInData(data, it)
    }
}

COUNTER.sort().each { k, v ->
    FREQUENCY_REPEATED_SUBORDINATEUNIT.println("${k},${v[0]},${v.tail()}")
}
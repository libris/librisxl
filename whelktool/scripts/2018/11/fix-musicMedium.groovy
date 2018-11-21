MUSICMEDIUM_TYPE = 'MusicMedium'
MUSICMEDIUM_PROPERTY = 'musicMedium'


boolean remodelToStructuredValue(key, term) {

    //Guard against updating definitions data or musicMedium with correct structure (added via the viewer)
    //musicMedium shall consist of a list of strings, otherwise ignore.
    if (key != MUSICMEDIUM_PROPERTY || !(term[MUSICMEDIUM_PROPERTY] instanceof List))
        return false

    if (term[MUSICMEDIUM_PROPERTY].any { !(it instanceof String)}) {
        println("${MUSICMEDIUM_PROPERTY} is not a list of strings. Skipping term...")
        return false
    }

    //Create musicMedium object and add to list
    term[MUSICMEDIUM_PROPERTY] = term[MUSICMEDIUM_PROPERTY].collect {
        ['@type': MUSICMEDIUM_TYPE, 'label': it]
    }
    return true

}

void findAndFixValuesInData(data, obj) {
    if (obj instanceof List) {
        obj.each {
            findAndFixValuesInData(data, it)
        }
    }
    else if (obj instanceof Map) {
        obj.each { key, value ->
            checkValueInData(data, obj, key, value)
        }
    }
}

void checkValueInData(data, container, key, value) {

    if (remodelToStructuredValue(key, container))
        data.scheduleSave()
    else {
        findAndFixValuesInData(data, value)
    }
}


selectBySqlWhere('''
        data::text LIKE '%"musicMedium"%'
        ''') { data ->

    // guard against missing entity
    if (data.graph.size() < 2) {
        return
    }

    // Skipping record
    data.graph[1..-1].each {
        findAndFixValuesInData(data, it)
    }
}

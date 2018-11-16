MUSICMEDIUM_TYPE = 'MusicMedium'
MUSICMEDIUM_PROPERTY = 'musicMedium'


boolean remodelToStructuredValue(term) {

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

    if (key == MUSICMEDIUM_PROPERTY) {
        if (remodelToStructuredValue(container))
            data.scheduleSave()
    }
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

MUSICMEDIUM_TYPE = 'MusicMedium'
MUSICMEDIUM_PROPERTY = 'musicMedium'


boolean remodelToStructuredValue(data, obj) {

    //Guard against updating definitions data or musicMedium with correct structure (added via the viewer)
    if (!(obj[MUSICMEDIUM_PROPERTY] instanceof List))
        return

    //musicMedium shall consist of a list of strings, otherwise ignore.
    if (obj[MUSICMEDIUM_PROPERTY].any { !(it instanceof String)}) {
        println("${MUSICMEDIUM_PROPERTY} is not a list of strings in post ${data.graph[0][ID]}. Skipping term...")
        return
    }

    //Create musicMedium object and add to list
    obj[MUSICMEDIUM_PROPERTY] = obj[MUSICMEDIUM_PROPERTY].collect {
        ['@type': MUSICMEDIUM_TYPE, 'label': it]
    }

    data.scheduleSave()
}

void findAndFixValuesInData(data, obj) {
    if (obj instanceof List) {
        obj.each {
            findAndFixValuesInData(data, it)
        }
    } else if (obj instanceof Map) {
        obj.each { key, value ->
            if (key == MUSICMEDIUM_PROPERTY) {
                remodelToStructuredValue(data, obj)
            } else {
                findAndFixValuesInData(data, value)
            }
        }
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

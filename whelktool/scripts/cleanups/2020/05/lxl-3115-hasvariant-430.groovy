class Script {
    static PrintWriter scheduledForUpdate
    static PrintWriter failed
    static PrintWriter mapped
}

Script.scheduledForUpdate = getReportWriter("scheduled-for-update")
Script.failed = getReportWriter("failed")
Script.mapped = getReportWriter("mapped")

selectByCollection('auth') { auth ->
    try {
        def _430s = auth.graph[0].get('_marcUncompleted')?.findResults{ it.get('430') }

        if(_430s) {
            auth.graph[1]['hasVariant'] = auth.graph[1]['hasVariant'] ?: []
            auth.graph[1]['hasVariant'].addAll(_430s.collect{ toVariant(auth.doc, it) })

            auth.graph[0].get('_marcUncompleted').removeAll { it.get('430') }

            auth.scheduleSave()
            Script.scheduledForUpdate.println(auth.doc.getURI())
        }
    }
    catch (Exception e) {
        Script.failed.println(e)
    }

}

Map toVariant(doc, Map _430) {
    Map variant = ['@type': 'Work']

    _430.subfields.each { Map subfield ->
        assert subfield.size() == 1
        subfield.each { String key, String value ->
            switch(key) {
                case 'a':
                    addTitleIfMissing(variant)
                    putValue(variant['hasTitle'][0], 'mainTitle', value)
                    break

                case 'p':
                    addTitleIfMissing(variant)
                    putInList( variant['hasTitle'][0], 'partName', value)
                    break

                case 'n':
                    addTitleIfMissing(variant)
                    putInList(variant['hasTitle'][0], 'partNumber', value)
                    break

                case 'l':
                    assert !variant['language']
                    variant['language'] = [['@type': 'Language', 'label': value]]
                    break

                case 'd':
                    putInList(variant, 'legalDate', value)
                    break

                case 'w':
                    putValue(variant, 'marc:controlSubfield', value)
                    break

                case 'k':
                    putInList(variant, 'natureOfContent', value)
                    break

                case 'g':
                    putValue(variant, 'place', value)
                    break

                case 'f':
                    putValue(variant, 'originDate', value)
                    break

                case 's':
                case 't':
                    putValue(variant, 'version', value)
                    break

                case 'x':
                    // drop
                    break

                default:
                    throw new RuntimeException("${doc.getURI()} Unhandled subfield $key : $value")
            }
        }
    }

    if(Integer.parseInt(_430['ind2']) in 1..9) {
        addTitleIfMissing(variant)
        putValue(variant['hasTitle'][0], 'marc:nonfilingChars', "${_430['ind2']}")
    }

    Script.mapped.println("${doc.getURI()}\n$_430\n$variant\n")
    
    return variant
}

void putInList(thing, key, value) {
    thing[key] = thing[key] ?: []
    thing[key].add(value)
}

void putValue(thing, key, value) {
    assert !thing[key]
    thing[key] = value
}

void addTitleIfMissing(variant) {
    variant['hasTitle'] = variant['hasTitle'] ?: [['@type': 'Title']]
}
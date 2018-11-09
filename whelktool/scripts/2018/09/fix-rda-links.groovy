brokenBase = 'http://id.kb.se/term/rda/'
correctBase = 'https://id.kb.se/term/rda/'

correctIdMap = [:]

String findCorrectId(String id) {
    if (id in correctIdMap) {
        return correctIdMap[id]
    }
    String correctId = correctIdMap[id] = findCanonicalId(id)
    return correctId
}

List asList(o) {
    return (o instanceof List) ? (List) o : o != null ? [o] : []
}

boolean fixRdaLink(ref) {
    Map brokenRef = ref.sameAs?.find { it[ID].startsWith(brokenBase) }
    String fixedId = null

    if (brokenRef) {
        def brokenId = brokenRef[ID]
        fixedId = correctBase + brokenId.substring(brokenBase.size())
    } else if (ref[TYPE] == 'MediaType') {
        def tokens = asList(ref.code) ?: asList(ref.label)
        if (tokens) {
            fixedId = correctBase + 'media/' + tokens[0]
        }
    }

    if (fixedId) {
        String correctId = findCorrectId(fixedId)
        if (correctId) {
            ref.clear()
            ref[ID] = correctId
        } else { // Not a correct term, but shape of URI is correct
            // TODO: really though? We could just remove it...
            brokenRef[ID] = fixedId
        }
        return true
    }
    return false
}

boolean fixRdaLinks(List <Map> references) {
    boolean fixed = false
    references.each {
        if (fixRdaLink(it)) {
            fixed = true
        }
    }
    if (fixed) {
        // Eliminate duplicate references:
        Set seenRefs = new HashSet()
        references.removeAll { !seenRefs.add(it[ID]) }
        return true
    }
    return false
}

//selectByIds(['h1t5mmst1k95p7w', 'fzrs2qjr3mb4fkk']) { data ->
selectBySqlWhere('''
    data#>'{@graph,1}' ??| array['carrierType', 'mediaType'] OR
    data#>'{@graph,2}' ?? 'contentType'
''') { data ->
    def (record, instance, work) = data.graph

    if (!isInstanceOf(instance, 'Instance')) {
        return
    }
    if (!work) {
        return
    }
    assert work['@id'] == instance.instanceOf['@id']

    [
        instance.mediaType,
        instance.carrierType,
        work.contentType
    ].each {
        if (fixRdaLinks(it)) {
            data.scheduleSave(loud: false)
        }
    }
}

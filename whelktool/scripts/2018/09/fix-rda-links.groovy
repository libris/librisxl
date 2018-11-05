brokenBase = 'http://id.kb.se/term/rda/'
correctBase = 'https://id.kb.se/term/rda/'

correctIdMap = [:]

String findCorrectId(String id) {
    if (id in correctIdMap) {
        return correctIdMap[id]
    }
    def correctId = correctIdMap[id] = findCanonicalId(id)
    return correctId
}

boolean fixRdaLink(ref) {
    def brokenRef = ref.sameAs?.find { it[ID].startsWith(brokenBase) }
    if (brokenRef) {
        def brokenId = brokenRef[ID]
        def fixedId = correctBase + brokenId.substring(brokenBase.size())
        def correctId = findCorrectId(fixedId)
        if (correctId) {
            ref.clear()
            ref[ID] = correctId
        } else { // Not a correct term, but shape of URI is correct
            brokenRef[ID] = fixedId
        }
        return true
    }
    return false
}

selectByCollection('bib') { data ->
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
        it.each {
            if (fixRdaLink(it)) {
                // TODO: when correct, eliminate possible duplicate in List
                data.scheduleSave(loud: false)
            }
        }
    }
}

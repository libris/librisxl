brokenBase = 'http://id.kb.se/rda/'
correctBase = 'https://id.kb.se/rda/'

rdaTerms = new HashSet()

boolean fixRdaLink(ref) {
    def brokenRef = ref.sameAs?.find { it[ID].startsWith(brokenBase) }
    if (brokenRef) {
        def brokenId = brokenRef.get[ID]
        def fixedId = correctBase + brokenId.substring(brokenBase.size())
        if (fixedId in rdaTerms || load(fixedId)) {
            ref.clear()
            ref[ID] = fixedId
            rdaTerms << fixedId
        } else { // Not a correct term, but shape of URI is correct
            brokenRef[ID] = fixedId
        }
        return true
    }
    return false
}

selectByCollection('bib') { data ->
    def (record, instance, work) = data.graph

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
                data.scheduleSave()
            }
        }
    }
}

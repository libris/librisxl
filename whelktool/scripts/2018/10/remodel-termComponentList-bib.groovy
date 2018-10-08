def subdivisionTypes = [
        'GenreForm' : 'GenreSubdivision',
        'Temporal' : 'TemporalSubdivision',
        'Geographic' : 'GeographicSubdivision'
]

process { data ->
    def (record, instance, work) = data.graph

    if (!work) return
    assert work['@id'] == instance.instanceOf['@id']

    work.subject.findAll {
        it['@type'] == 'ComplexSubject'
    }.each {
        it.termComponentList.eachWithIndex { term, i ->
            //Can we know for sure that the first term always is the main term?
            // If not, how can we determine which one the main term is?
            if (i != 0) {
                if (subdivisionTypes[term['@type']]){
                    term['@type'] = subdivisionTypes[term['@type']]
                    data.scheduleSave()
                }
            }
        }
    }
}

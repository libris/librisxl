subdivisionTypes = [
        'GenreForm' : 'GenreSubdivision',
        'Temporal' : 'TemporalSubdivision',
        'Geographic' : 'GeographicSubdivision'
]

COMPLEX_SUBJECT_TYPE = 'ComplexSubject'

boolean changeToSubdivision(it, subdivisionTypes) {
    boolean changed = false
    it.termComponentList.eachWithIndex { term, i ->
        if (i != 0) {
            if (subdivisionTypes[term['@type']]) {
                term['@type'] = subdivisionTypes[term['@type']]
                changed = true
            }
        }
    }
    return changed
}

selectBySqlWhere('''
        data::text LIKE '%"termComponentList"%'
        ''') { data ->
    // guard against missing entity
    if (data.graph.size() < 2) {
        return
    }
    // bib
    if (data.graph[1].containsKey('instanceOf')) {
        def (record, instance, work) = data.graph

        if (!work) return
        assert work['@id'] == instance.instanceOf['@id']

        work.subject.findAll {
            it['@type'] == COMPLEX_SUBJECT_TYPE
        }.each {
            if (changeToSubdivision(it, subdivisionTypes)) {
                data.scheduleSave()
            }
        }
    } else {
        // auth
        def (record, authdata) = data.graph

        if (!authdata) return

        if (authdata['@type'] == COMPLEX_SUBJECT_TYPE) {
            if (changeToSubdivision(authdata, subdivisionTypes)) {
                data.scheduleSave()
            }
        }
    }

}

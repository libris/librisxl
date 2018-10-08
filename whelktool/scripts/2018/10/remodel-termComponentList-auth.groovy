def subdivisionTypes = [
        'GenreForm' : 'GenreSubdivision',
        'Temporal' : 'TemporalSubdivision',
        'Geographic' : 'GeographicSubdivision'
]

process { data ->
    def (record, authdata) = data.graph

    if (!authdata) return

    if (authdata['@type'] == 'ComplexSubject') {
        authdata.termComponentList.eachWithIndex { term, i ->
            if (i != 0) {
                if (subdivisionTypes[term['@type']]){
                    term['@type'] = subdivisionTypes[term['@type']]
                    data.scheduleSave()
                }
            }
        }
    }
}

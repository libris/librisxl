/**
 * Example of setting meta.inDataset based on query
 */

def suecia =
        [
                dataset: '6l5swjj84dcg9rhw',
                query  :
                        [
                                // This doesn't find everything as currently the Electronic records are not in the bibliographies... 
                                '@type'                : ['Electronic'],
                                'meta.bibliography.@id': ['https://libris.kb.se/bibliography/SAH'],
                                'meta.bibliography.@id': ['https://libris.kb.se/bibliography/SAHF'],
                                'meta.bibliography.@id': ['https://libris.kb.se/bibliography/SAHT'],
                                '_sort'                : ['@id'],
                        ]
        ]

run(suecia)

void run(Map spec) {
    String datasetUri = getIri(spec.dataset)
    
    selectByIds(queryIds(spec.query).collect()) { d ->
        def (record, _thing) = d.graph
        if (setDataset(record, datasetUri)) {
            d.scheduleSave()
        }
    }
}

boolean setDataset(Map record, datasetUri) {
    Set inDatasets = asList(record.inDataset) as Set
    def link = [ '@id': datasetUri ]
    boolean modified = inDatasets.add(link)
    record.inDataset = inDatasets as List
    return modified
}

String getIri(String id) {
    String iri
    selectByIds([id]) { d ->
        iri = d.doc.getThingIdentifiers().first()
    }
    if (!iri) {
        throw new RuntimeException("No iri found for $id")
    }
    return iri
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
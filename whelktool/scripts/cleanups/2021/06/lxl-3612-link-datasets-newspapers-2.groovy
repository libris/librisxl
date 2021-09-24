// UPDATE DATASET IDS FOR PROD

import java.util.function.Predicate

def datasets =
        [
                [
                        dataset: 'l0sqlrn8jcp5svln',
                        query  :
                                [
                                        '@type'                     : ['Electronic'],
                                        'hasTitle.mainTitle'        : ['POSTTIDNINGAR'],
                                        'min-publication.year'      : ['1645'],
                                        'max-publication.year'      : ['1705'],
                                        '@reverse.itemOf.heldBy.@id': ['https://libris.kb.se/library/APIS'],
                                        '_sort'                     : ['@id']
                                ],
                        predicate: { doc ->
                            def (record, thing) = doc.graph
                            return hasTitle(thing, /POSTTIDNINGAR\s+\d\d\d\d-\d\d\-\d\d/) && hasControlNumber(thing, '11653825')
                        }
                ],
           
        ]

datasets.each {
    run(it)    
}

void run(Map spec) {
    String datasetUri = getIri(spec.dataset)

    selectByIds(queryIds(spec.query).collect()) { d ->
        def (record, _thing) = d.graph
        Predicate predicate = spec.predicate 
        if (predicate.test(d) && setDataset(record, datasetUri)) {
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

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}

boolean hasControlNumber(Map thing, String controlNumber) {
    asList(thing.supplementTo).any { Map instance ->
        asList(instance.describedBy).any { Map r -> r.controlNumber == controlNumber }
    }
}

boolean hasTitle(Map thing, String pattern) {
    asList(thing.hasTitle).any { Map title ->
        title.mainTitle ==~ pattern
    }
}
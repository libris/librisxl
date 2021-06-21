// UPDATE DATASET IDS FOR PROD

import java.util.function.Predicate

def datasets =
        [
                [
                        dataset: '7mf87n2g5xzjf8r4',
                        query  :
                                [
                                        '@type'                     : ['Electronic'],
                                        'hasTitle.mainTitle'        : ['AFTONBLADET'],
                                        'min-publication.year'      : ['1831'],
                                        'max-publication.year'      : ['1900'],
                                        '@reverse.itemOf.heldBy.@id': ['https://libris.kb.se/library/APIS'],
                                        '_sort'                     : ['@id']
                                ],
                        predicate: { doc ->
                            def (record, thing) = doc.graph
                            return hasTitle(thing, /AFTONBLADET\s+\d\d\d\d-\d\d\-\d\d/) && hasControlNumber(thing, '4345612')
                        }
                ],
                [
                        dataset: '6ld76n1p44s12ht1',
                        query  :
                                [
                                        '@type'                     : ['Electronic'],
                                        'hasTitle.mainTitle'        : ['DAGENS NYHETER'],
                                        'min-publication.year'      : ['1864'],
                                        'max-publication.year'      : ['1900'],
                                        '@reverse.itemOf.heldBy.@id': ['https://libris.kb.se/library/APIS'],
                                        '_sort'                     : ['@id']
                                ],
                        predicate: { doc ->
                            def (record, thing) = doc.graph

                            return hasTitle(thing, /DAGENS NYHETER\s+\d\d\d\d-\d\d\-\d\d/) && hasControlNumber(thing, '13991099')
                        }
                ],
                [
                        dataset: 'hwpjh1n7fnsskvfr',
                        query  :
                                [
                                        '@type'                     : ['Electronic'],
                                        'hasTitle.mainTitle'        : ['NORRKÖPINGS WECKOTIDNINGAR'],
                                        'min-publication.year'      : ['1758'],
                                        'max-publication.year'      : ['1786'],
                                        '@reverse.itemOf.heldBy.@id': ['https://libris.kb.se/library/APIS'],
                                        '_sort'                     : ['@id']
                                ],
                        predicate: { doc ->
                            def (record, thing) = doc.graph

                            return hasTitle(thing, /NORRKÖPINGS WECKOTIDNINGAR\s+\d\d\d\d-\d\d\-\d\d/) && hasControlNumber(thing, '19227357')
                        }
                ],
                [
                        dataset: 'm1tnm4cpktxgf28n',
                        query  :
                                [
                                        '@type'                     : ['Electronic'],
                                        'hasTitle.mainTitle'        : ['NORRKÖPINGS TIDNINGAR'],
                                        'min-publication.year'      : ['1787'],
                                        'max-publication.year'      : ['1895'],
                                        '@reverse.itemOf.heldBy.@id': ['https://libris.kb.se/library/APIS'],
                                        '_sort'                     : ['@id']
                                ],
                        predicate: { doc ->
                            def (record, thing) = doc.graph
                            
                            return hasTitle(thing, /NORRKÖPINGS TIDNINGAR\s+\d\d\d\d-\d\d\-\d\d/) && hasControlNumber(thing, '15650837')
                        }
                ],
                [
                        dataset: 'hwpjh11gfnk12r2z',
                        query  :
                                [
                                        '@type'                     : ['Electronic'],
                                        'hasTitle.mainTitle'        : ['SVENSKA DAGBLADET'],
                                        'min-publication.year'      : ['1884'],
                                        'max-publication.year'      : ['1900'],
                                        '@reverse.itemOf.heldBy.@id': ['https://libris.kb.se/library/APIS'],
                                        '_sort'                     : ['@id']
                                ],
                        predicate: { doc ->
                            def (record, thing) = doc.graph

                            return hasTitle(thing, /SVENSKA DAGBLADET\s+\d\d\d\d-\d\d\-\d\d/) && hasControlNumber(thing, '13434192')
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
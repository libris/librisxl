// UPDATE DATASET IDS FOR PROD

import java.util.function.Predicate

def datasets =
        [
                [
                        dataset: 'n2rgzfcklj9zcwlw',
                        query  :
                                [
                                        '@type'                     : ['Electronic'],
                                        'hasTitle.mainTitle'        : ['AFTONBLADET'],
                                        'min-publication.year'      : ['1832'],
                                        'max-publication.year'      : ['1900'],
                                        '@reverse.itemOf.heldBy.@id': ['https://libris.kb.se/library/APIS'],
                                        '_sort'                     : ['@id']
                                ],
                        predicate: { doc ->
                            def (record, thing) = doc.graph
                            
                            asList(thing.hasTitle).any { Map title ->
                                title.mainTitle ==~ /AFTONBLADET \d\d\d\d-\d\d\-\d\d/
                            }
                        }
                ],
                [
                        dataset: 'm1qfz6d3k163n1ww',
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

                            asList(thing.hasTitle).any { Map title ->
                                title.mainTitle ==~ /DAGENS NYHETER \d\d\d\d-\d\d\-\d\d/
                            }
                        }
                ],
                [
                        dataset: 'w90p8vw6tcph9xl3',
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

                            asList(thing.hasTitle).any { Map title ->
                                title.mainTitle ==~ /NORRKÖPINGS WECKOTIDNINGAR \d\d\d\d-\d\d\-\d\d/
                            }
                        }
                ],
                [
                        dataset: 'l0pdzslgjcdk19cx',
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

                            asList(thing.hasTitle).any { Map title ->
                                title.mainTitle ==~ /NORRKÖPINGS TIDNINGAR \d\d\d\d-\d\d\-\d\d/
                            }
                        }
                ],
                [
                        dataset: 'ftj7swkgcxxbm982',
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

                            asList(thing.hasTitle).any { Map title ->
                                title.mainTitle ==~ /SVENSKA DAGBLADET \d\d\d\d-\d\d\-\d\d/
                            }
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
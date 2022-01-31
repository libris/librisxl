/**
 * Adapted from scripts/cleanups/2021/06/lxl-3612-link-datasets-newspapers.groovy
 * 
 * More lax criteria (don't check Mimer holding etc) to be run on provided list of docs
 */

import java.util.function.Predicate

report = getReportWriter("report.txt")

def datasets =
        [
                [
                        dataset: '7mf87n2g5xzjf8r4',
                        predicate: { doc ->
                            def (record, thing) = doc.graph
                            return hasTitle(thing, /AFTONBLADET\s+\d\d\d\d-\d\d\-\d\d/)
                        }
                ],
                [
                        dataset: '6ld76n1p44s12ht1',
                        predicate: { doc ->
                            def (record, thing) = doc.graph

                            return hasTitle(thing, /DAGENS\s+NYHETER\s+\d\d\d\d-\d\d\-\d\d/)
                        }
                ],
                [
                        dataset: 'hwpjh1n7fnsskvfr',
                        predicate: { doc ->
                            def (record, thing) = doc.graph

                            return hasTitle(thing, /NORRKÖPINGS\s+WECKOTIDNINGAR\s+\d\d\d\d-\d\d\-\d\d/)
                        }
                ],
                [
                        dataset: 'm1tnm4cpktxgf28n',
                        predicate: { doc ->
                            def (record, thing) = doc.graph

                            return hasTitle(thing, /NORRKÖPINGS\s+TIDNINGAR\s+\d\d\d\d-\d\d\-\d\d/)
                        }
                ],
                [
                        dataset: 'hwpjh11gfnk12r2z',
                        predicate: { doc ->
                            def (record, thing) = doc.graph

                            return hasTitle(thing, /SVENSKA\s+DAGBLADET\s+\d\d\d\d-\d\d\-\d\d/)
                        }
                ],
        ]

datasets.each {
    it.datasetUri = getIri(it.dataset)
}

selectByIds( new File(scriptDir, 'ids.txt').readLines() ) { bib ->
    for (Map spec : datasets) {
        def (record, _thing) = bib.graph
        Predicate predicate = spec.predicate
        if (predicate.test(bib) && setDataset(record, spec.datasetUri)) {
            bib.scheduleSave()
            return
        }
    }

    report.println("not matched: $bib.doc.shortId ${title(bib)}")
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

boolean hasTitle(Map thing, String pattern) {
    asList(thing.hasTitle).any { Map title ->
        title.mainTitle ==~ pattern
    }
}

def title(item) {
    def (record, thing) = item.graph
    asList(thing.hasTitle)
}

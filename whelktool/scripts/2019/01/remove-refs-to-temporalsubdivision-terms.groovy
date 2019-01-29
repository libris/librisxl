import java.net.URI

TERMS_TO_CHANGE = ["medeltiden", "antiken", "forntiden", "renässansen"]
COMPLEX_SUBJECT_TYPE = 'ComplexSubject'
SAO_URI = 'https://id.kb.se/term/sao'


String termToUri(term) {
    return SAO_URI + '/' + URLEncoder.encode(term.capitalize(), "UTF-8").replaceAll("\\+", "%20")
}

boolean updateReference(work) {
    def extractedTerms = []
    def entitiesToMove = []

    if (!work.subject)
        return

    ListIterator iterSubj = work.subject.listIterator()
    while (iterSubj.hasNext()) {
        subj = iterSubj.next()

        if (subj['@type'] == COMPLEX_SUBJECT_TYPE && subj['inScheme'] && subj['inScheme']['@id'] == SAO_URI) {

            if (subj.termComponentList.any{ it['@type'] == 'TemporalSubdivision'}
                    && subj.termComponentList.any{ TERMS_TO_CHANGE.contains(it['prefLabel'])}) {

                ListIterator iter = subj.termComponentList.listIterator()

                while(iter.hasNext()) {
                    cpx_term = iter.next()

                    if (cpx_term['@type'] == 'TemporalSubdivision' && TERMS_TO_CHANGE.contains(cpx_term['prefLabel'].toLowerCase())) {
                        if (!extractedTerms.contains(termToUri(cpx_term['prefLabel']))) {
                            extractedTerms << termToUri(cpx_term['prefLabel'])
                        }
                        iter.remove()
                    }
                }
            }

            //Extract remaining entity in ComplexSubject, if only one remains
            if (subj.termComponentList.size() == 1) {
                def prefLabel = ""

                if (subj.termComponentList.get(0)['@id']) {
                    if (!extractedTerms.contains(subj.termComponentList.get(0)['@id'])) {
                        extractedTerms << subj.termComponentList.get(0)['@id']
                    }
                } else if (subj.termComponentList.get(0)['prefLabel']) {
                    prefLabel = subj.termComponentList.get(0)['prefLabel']
                } else {
                    entitiesToMove << subj.termComponentList.get(0)
                }

                if (!prefLabel.isEmpty() && !extractedTerms.contains(termToUri(prefLabel)))
                    extractedTerms << termToUri(prefLabel)

                iterSubj.remove()
            } else {
                // Update existing prefLabel and sameAs uri of ComplexSubject
                def prefLabelTerms = []

                subj.termComponentList.each {
                    if (it['@id']) {
                        def path = it['@id'].toURL().getPath()
                        prefLabelTerms << URLDecoder.decode(path.substring(path.lastIndexOf('/') + 1), "UTF-8")

                    } else if (it['prefLabel']) {
                        prefLabelTerms << it['prefLabel']
                    }
                    //TODO: What if there is np termComponentList.prefLabel? When creating subject.prefLabel and uris.
                    // Should probably not find a ComplexSubject with (main) prefLabel and
                    // sameAs if main term is Work/Person/Organization etc
                    //prefLabelTerms << it['prefLabel'] ?: it['name']
                }

                if (subj.prefLabel) {
                    subj.prefLabel = prefLabelTerms.join("--")
                }

                if (subj['sameAs']) {
                    subj['sameAs'].get(0)['@id'] = termToUri(prefLabelTerms.join("--"))
                }
            }

        }
    }



    if (extractedTerms.size() || entitiesToMove.size()) {
        def existingUris = []

        work.subject.each {
            if (it['@id']) {
                existingUris << it['@id']
            }
        }

        if (extractedTerms.size()) {
            extractedTerms.each {
                if (!existingUris.contains(it))
                    work.subject << ['@id': it]
            }
        }

        if (entitiesToMove.size()) {
            entitiesToMove.each {
                work.subject << it
            }
        }
        return true
    }
}

selectBySqlWhere("data::text LIKE '%termComponentList%' and (data::text LIKE '%medeltiden%' or data::text LIKE '%antiken%' " +
            "or data::text LIKE '%forntiden%' or data::text LIKE '%renässansen%')") { data ->

    // guard against missing entity
    if (data.graph.size() < 2) {
        return
    }
    // bib and hold

    if (data.graph[1].containsKey('instanceOf')) {
        def (record, instance, work) = data.graph

        if (!work) return
        assert work['@id'] == instance.instanceOf['@id']

        if (updateReference(work)) {
            data.scheduleSave()
        }


    }
}

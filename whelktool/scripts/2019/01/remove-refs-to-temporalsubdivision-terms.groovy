TERMS_TO_CHANGE = ["medeltiden", "antiken", "forntiden", "renässansen"]
COMPLEX_SUBJECT_TYPE = 'ComplexSubject'
SAO_URI = 'https://id.kb.se/term/sao'
PREFLABEL = 'prefLabel'
TEMPSUB_TYPE = 'TemporalSubdivision'


String termToUri(term) {
    return SAO_URI + '/' + URLEncoder.encode(term.capitalize(), "UTF-8").replaceAll("\\+", "%20")
}
/*
String getIdOfTerm(term) {
    selectBySqlWhere("""
            collection = 'auth' AND data#>>'{@graph,1,@type}' = 'Geographic' AND data#>>'{@graph,1,prefLabel}' = '${term}'
    """) { data ->
        return data[1][ID]
    }
}*/

boolean updateReference(work) {
    def extractedTerms = []
    def entitiesToMove = []

    if (!work.subject)
        return

    ListIterator iterSubj = work.subject.listIterator()
    while (iterSubj.hasNext()) {
        subj = iterSubj.next()

        if (subj[TYPE] == COMPLEX_SUBJECT_TYPE && subj['inScheme'] && subj['inScheme'][ID] == SAO_URI
            && subj.termComponentList.any{ it[TYPE] == TEMPSUB_TYPE} &&
                subj.termComponentList.any{ TERMS_TO_CHANGE.contains(it[PREFLABEL])}) {

            ListIterator iter = subj.termComponentList.listIterator()

            while(iter.hasNext()) {
                cpx_term = iter.next()

                if (cpx_term[TYPE] == TEMPSUB_TYPE && TERMS_TO_CHANGE.contains(cpx_term[PREFLABEL].toLowerCase())) {
                    if (!extractedTerms.contains(termToUri(cpx_term[PREFLABEL]))) {
                        extractedTerms << termToUri(cpx_term[PREFLABEL])
                    }
                    iter.remove()
                }
            }

            //Extract remaining entity in ComplexSubject, if only one remains
            if (subj.termComponentList.size() == 1) {
                if (subj.termComponentList.get(0)[ID]) {
                    if (!extractedTerms.contains(subj.termComponentList.get(0)[ID])) {
                        extractedTerms << subj.termComponentList.get(0)[ID]
                    }
                } else if (subj.termComponentList.get(0)[PREFLABEL]) {
                    newUri = termToUri(subj.termComponentList.get(0)[PREFLABEL])
                    if (findCanonicalId(newUri)) {
                        if (!extractedTerms.contains(newUri))
                            extractedTerms << newUri
                    } //else if (subj.termComponentList.get(0)[TYPE] == 'Geographic') {
                        //geoUri = getIdOfTerm(subj.termComponentList.get(0)[PREFLABEL])
                        //if (!extractedTerms.contains(geoUri))
                        //    extractedTerms << geoUri
                    //}
                    else {
                        if (!entitiesToMove.size() || entitiesToMove.any{ it[TYPE] != subj.termComponentList.get(0)[TYPE] ||
                                it['label'] != subj.termComponentList.get(0)[PREFLABEL]})
                            entitiesToMove << [(TYPE): subj.termComponentList.get(0)[TYPE],
                                           'label': subj.termComponentList.get(0)[PREFLABEL]]
                    }
                } else {
                    entitiesToMove << subj.termComponentList.get(0)
                }
                iterSubj.remove()
            } else {
                // Update existing prefLabel and sameAs uri of ComplexSubject
                def prefLabelTerms = []

                subj.termComponentList.each {
                    if (it[ID]) {
                        doc = load(it[ID])
                        if (doc && doc[GRAPH][1].containsKey(PREFLABEL)) {
                            prefLabelTerms << doc[GRAPH][1].prefLabel
                        }
                    } else if (it[PREFLABEL]) {
                        prefLabelTerms << it[PREFLABEL]
                    }
                }

                if (subj.prefLabel) {
                    subj.prefLabel = prefLabelTerms.join("--")
                }

                if (subj['sameAs']) {
                    subj['sameAs'].get(0)[ID] = termToUri(prefLabelTerms.join("--"))
                }
            }

        }
    }


    if (extractedTerms.size() || entitiesToMove.size()) {
        def existingUris = []

        work.subject.each {
            if (it[ID]) {
                existingUris << it[ID]
            }
        }

        if (extractedTerms.size()) {
            extractedTerms.each {
                if (!existingUris.contains(it))
                    work.subject << [(ID): it]
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

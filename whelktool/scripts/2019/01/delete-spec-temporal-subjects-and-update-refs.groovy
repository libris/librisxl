COMPLEX_SUBJECT_TYPE = 'ComplexSubject'
SAO_URI = 'https://id.kb.se/term/sao'
PREFLABEL = 'prefLabel'
TEMPSUB_TYPE = 'TemporalSubdivision'
SUBJECTS_TO_DELETE = ['pm14bcq70h0qnnr': 'medeltiden',
                      '53hlst5p58lc91k': 'forntiden',
                      '64jmtv6q2v2t5b9': 'antiken',
                      '75knvw8r0qh4c88': 'renässansen']


PrintWriter failedAuthIDs = getReportWriter("failed-to-delete-authIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")


String termToUri(term) {
    return SAO_URI + '/' + URLEncoder.encode(term.capitalize(), "UTF-8").replaceAll("\\+", "%20")
}

String getIdOfTerm(term) {
    String uri
    selectBySqlWhere("""
            collection = 'auth' AND data#>>'{@graph,1,@type}' = 'Geographic' AND data#>>'{@graph,1,prefLabel}' = '${term.capitalize()}'
    """) { doc ->
        uri = doc.graph[1][ID]
    }
    return uri
}

List extractRemainingTerm(termComponentList) {
    List listOfEntities = []
    if (termComponentList.get(0)[ID]) {
        listOfEntities << termComponentList.get(0)[ID]
    } else if (termComponentList.get(0)[PREFLABEL]) {
        String newUri

        if (termComponentList.get(0)[TYPE] == 'Geographic')
            newUri = getIdOfTerm(termComponentList.get(0)[PREFLABEL])
        else
            newUri = findCanonicalId(termToUri(termComponentList.get(0)[PREFLABEL]))

        if (newUri)
            listOfEntities << newUri
    }
    return listOfEntities
}

void setPrefLabelAndSameAs(subj) {
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

boolean updateReference(work) {
    def extractedTerms = []
    def entitiesToMove = []
    def termsToChange = SUBJECTS_TO_DELETE.values() as List

    if (!work.subject) return

    ListIterator iterSubj = work.subject.listIterator()
    while (iterSubj.hasNext()) {
        subj = iterSubj.next()

        if (subj[TYPE] == COMPLEX_SUBJECT_TYPE && subj['inScheme'] && subj['inScheme'][ID] == SAO_URI
            && subj.termComponentList.any{ it[TYPE] == TEMPSUB_TYPE}
                && subj.termComponentList.any{ termsToChange.contains(it[PREFLABEL]?.toLowerCase())}) {

            ListIterator iter = subj.termComponentList.listIterator()

            while(iter.hasNext()) {
                cpx_term = iter.next()
                if (cpx_term[TYPE] == TEMPSUB_TYPE && termsToChange.contains(cpx_term[PREFLABEL].toLowerCase())) {
                    if (!extractedTerms.contains(termToUri(cpx_term[PREFLABEL]))) {
                        extractedTerms << termToUri(cpx_term[PREFLABEL])
                    }
                    iter.remove()
                }
            }

            //Extract remaining entity in ComplexSubject, if only one remains
            if (subj.termComponentList.size() == 1) {
                def termsToMove = extractRemainingTerm(subj.termComponentList)
                if(termsToMove) {
                    termsToMove.each {
                        if (!extractedTerms.contains(it)) {
                            extractedTerms << it
                        }
                    }
                } else if (subj.termComponentList.get(0)[PREFLABEL]) {
                    //Special treatment of geographical local subjects which shall be able to export to bib 651.
                    //Otherwise follow convert to label to be able to export to bib 653
                    String keyLabel = 'label'
                    if (subj.termComponentList.get(0)[TYPE] == 'Geographic') { keyLabel = 'prefLabel' }

                    if (entitiesToMove.empty || entitiesToMove.any{ it[TYPE] != subj.termComponentList.get(0)[TYPE] &&
                            it[keyLabel] != subj.termComponentList.get(0)[PREFLABEL].capitalize()}) {
                        entitiesToMove << [(TYPE): subj.termComponentList.get(0)[TYPE],
                                           (keyLabel): subj.termComponentList.get(0)[PREFLABEL].capitalize()]
                    }
                } else {
                    entitiesToMove << subj.termComponentList.get(0)
                }

                iterSubj.remove()
            } else {
                // Update existing prefLabel and sameAs uri of ComplexSubject
                setPrefLabelAndSameAs(subj)
            }
        }
    }


    if (!extractedTerms.empty || !entitiesToMove.empty) {
        def existingUris = []
        work.subject.each {
            if (it[ID]) {
                existingUris << it[ID]
            }
        }
        if (!extractedTerms.empty) {
            extractedTerms.each {
                if (!existingUris.contains(it))
                    work.subject << [(ID): it]
            }
        }
        if (!entitiesToMove.empty) {
            entitiesToMove.each {
                work.subject << it
            }
        }
        return true
    }
}

//Remove Temporal Subjects 'Medeltiden', 'Forntiden', 'Antiken' and 'Renässansen'
selectByIds( SUBJECTS_TO_DELETE.keySet() as List ) { auth ->
    if (SUBJECTS_TO_DELETE[auth.doc.shortId] == auth.graph[1]['prefLabel'].toLowerCase()
        && auth.graph[1][TYPE] == 'Temporal') {
        scheduledForDeletion.println("${auth.doc.getURI()}")
        auth.scheduleDelete(onError: { e ->
            failedAuthIDs.println("Failed to delete ${auth.doc.shortId} due to: $e")
        })
    }
}

selectBySqlWhere('''
        collection in ('bib', 'hold') AND data::text LIKE '%termComponentList%' 
        AND (data::text LIKE '%medeltiden%' OR data::text LIKE '%antiken%' 
        OR data::text LIKE '%forntiden%' OR data::text LIKE '%renässansen%')
    ''') { data ->

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

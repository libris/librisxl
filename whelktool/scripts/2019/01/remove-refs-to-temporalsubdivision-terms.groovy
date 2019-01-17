import java.awt.List

TERMS_TO_CHANGE = ["medeltiden", "antiken", "forntiden", "renässansen"]
COMPLEX_SUBJECT_TYPE = 'ComplexSubject'
SAO_URI = 'https://id.kb.se/term/sao'



boolean updateReference(work) {
    def absorbedTerms = []

    ListIterator iterSubj = work.subject.listIterator()
    while (iterSubj.hasNext()) {
        subj = iterSubj.next()

        if (subj['@type'] == COMPLEX_SUBJECT_TYPE && subj['inScheme']['@id'] == SAO_URI) { //&& subj['inScheme'] 

            if (subj.termComponentList.any{ it['@type'] == 'TemporalSubdivision'}
                    && subj.termComponentList.any{ TERMS_TO_CHANGE.contains(it['prefLabel'])}) {

                ListIterator iter = subj.termComponentList.listIterator()

                while(iter.hasNext()) {
                    cpx_term = iter.next()

                    if (cpx_term['@type'] == 'TemporalSubdivision' && TERMS_TO_CHANGE.contains(cpx_term['prefLabel'].toLowerCase())) {
                        if (!absorbedTerms.contains(cpx_term['prefLabel']))
                            absorbedTerms << cpx_term['prefLabel']
                        iter.remove()

                    }
                }
            }

            if (subj.termComponentList.size() == 1) {
                def prefLabel = subj.termComponentList.get(0)['prefLabel'] ?: subj.termComponentList.get(0)['name']

                if (!absorbedTerms.contains(prefLabel))
                    absorbedTerms << prefLabel

                iterSubj.remove()
            } else {
                def prefLabelTerms = []
                subj.termComponentList.each {
                    prefLabelTerms << it['prefLabel'] ?: it['name']
                }

                // Update prefLabel
                if (subj.prefLabel) {
                    subj.prefLabel = prefLabelTerms.join("--")
                }

                // Update sameAs uri
                if (subj['sameAs']) {
                    subj['sameAs'].each {
                        def newUriTerm = URLEncoder.encode(subj.prefLabel, "UTF-8")
                        it['@id'] = SAO_URI + '/' + newUriTerm
                    }
                }
            }

        }
    }

    if (absorbedTerms.size() > 0) {
        absorbedTerms.each {
            work.subject << ['@id': SAO_URI + '/' + URLEncoder.encode(it, "UTF-8")]
        }
        return true
    }

    //TODO:
    // - %20 in url becomes '+' --> See //libris-qa.kb.se/2ldjczqd5sh4qs5 --> 14838612
    //Create new prefLabel and sameAs.id --> Not always a local entity. Need to handle @id reference. See //libris-qa.kb.se/3mfmt5wf4l7vfrm --> 16004233

}



selectBySqlWhere("data::text LIKE '%medeltiden%' or data::text LIKE '%antiken%' " +
        "or data::text LIKE '%forntiden%' or data::text LIKE '%renässansen%'") { data ->

    // guard against missing entity
    if (data.graph.size() < 2) {
        return
    }
    // bib and hold --> how about hold??
    if (data.graph[1].containsKey('instanceOf')) {
        def (record, instance, work) = data.graph

        if (!work) return
        assert work['@id'] == instance.instanceOf['@id']

        if (updateReference(work)) {
            data.scheduleSave()
        }


    }
}

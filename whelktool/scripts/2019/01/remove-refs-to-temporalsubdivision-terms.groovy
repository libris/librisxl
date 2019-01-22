//import java.awt.List
import java.net.URI

TERMS_TO_CHANGE = ["medeltiden", "antiken", "forntiden", "renässansen"]
COMPLEX_SUBJECT_TYPE = 'ComplexSubject'
SAO_URI = 'https://id.kb.se/term/sao'


boolean updateReference(work) {
    def absorbedTerms = []

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
                        if (!absorbedTerms.contains(cpx_term['prefLabel']))
                            absorbedTerms << cpx_term['prefLabel']
                        iter.remove()

                    }
                }
            }

            if (subj.termComponentList.size() == 1) {
                //TODO: Need to check if it's an @id
                def prefLabel = subj.termComponentList.get(0)['prefLabel'] ?: subj.termComponentList.get(0)['name']

                if (!absorbedTerms.contains(prefLabel))
                    absorbedTerms << prefLabel

                iterSubj.remove()
            } else {
                def prefLabelTerms = []

                subj.termComponentList.each {
                    if (it['@id']) {
                        def path = it['@id'].toURL().getPath()
                        //def path = uri.getPath()
                        prefLabelTerms << URLDecoder.decode(path.substring(path.lastIndexOf('/') + 1), "UTF-8")

                    } else {
                        prefLabelTerms << it['prefLabel'] ?: it['name']
                    }
                }

                // Update prefLabel
                if (subj.prefLabel) {
                    subj.prefLabel = prefLabelTerms.join("--")
                }

                // Update sameAs uri
                if (subj['sameAs']) {
                    subj['sameAs'].each {
                        def newUriTerm = URLEncoder.encode(subj.prefLabel, "UTF-8").replaceAll("\\+", "%20")
                        it['@id'] = SAO_URI + '/' + newUriTerm
                    }
                }
            }

        }
    }

    if (absorbedTerms.size() > 0) {
        def existingUris = []

        work.subject.each {
            if (it['@id']) {
                existingUris << it['@id']
            }
        }

        absorbedTerms.each {
            def newTerm = SAO_URI + '/' + URLEncoder.encode(it.capitalize(), "UTF-8").replaceAll("\\+", "%20")
            if (!existingUris.contains(newTerm))
                work.subject << ['@id': newTerm]
        }
        return true
    }

    //TODO:
    // - %20 in url becomes '+' --> See //libris-qa.kb.se/2ldjczqd5sh4qs5 --> 14838612
    //Done: Create new prefLabel and sameAs.id --> Not always a local entity. Need to handle @id reference. See //libris-qa.kb.se/3mfmt5wf4l7vfrm --> 16004233

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

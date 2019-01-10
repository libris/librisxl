TERMS_TO_CHANGE = ["medeltiden", "antiken", "forntiden", "renässansen"]
COMPLEX_SUBJECT_TYPE = 'ComplexSubject'
SAO_URI = 'https://id.kb.se/term/sao'


boolean updateReference(obj) {

    if (obj['inScheme']['@id'] && obj['inScheme']['@id'] != SAO_URI)
        return false

    if (obj.termComponentList.any{ it['@type'] == 'TemporalSubdivision'}
            && obj.termComponentList.any{ TERMS_TO_CHANGE.contains(it['prefLabel'])}) {

        ListIterator iter = obj.termComponentList.listIterator()
        def removedTerms = []

        while(iter.hasNext()) {
            it = iter.next()
            if(it['@type'] == 'TemporalSubdivision' && TERMS_TO_CHANGE.contains(it['prefLabel'].toLowerCase())) {
                removedTerms << it['prefLabel']
                iter.remove()
            }
        }

        // Update prefLabel, sameAs and add uri ref
        if (removedTerms.size() > 0 && obj.termComponentList.size() >= 2) {
            def prefLabelTerms = []
            obj.termComponentList.each {
                prefLabelTerms << it['prefLabel'] ?: it['name']
            }

            // Update prefLabel
            if (obj.prefLabel) {
                obj.prefLabel = prefLabelTerms.join("--")
            }

            // Update sameAs uri
            if (obj.sameAs) {
                obj.sameAs.each {
                    def uri = it['@id'].toURL()
                    def path = uri.getPath();
                    def segmentToUpdate = path.substring(path.lastIndexOf('/') + 1);

                    it['@id'] = it['@id'].replace(segmentToUpdate, obj.prefLabel)
                }
            }

            //lägg till en ny länk till entiteten direkt på subject
            //OBS!! Detta blir fel. Blir ett @id direkt på subject --> behöver omsluta det med {}
            removedTerms.each {
                obj << ['@id': SAO_URI + '/' + it]
            }

        }
        //Lägg till nytt @id med uri till termen
        if (obj.termComponentList.size() <= 1) {
            obj.termComponentList.each
            obj << ['@id': SAO_URI + '/' + it]
        }
    }
    return true
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

        work.subject.findAll {
            it['@type'] == COMPLEX_SUBJECT_TYPE
        }.each {
            if (updateReference(it)) {
                data.scheduleSave()
            }
        }
    }
}

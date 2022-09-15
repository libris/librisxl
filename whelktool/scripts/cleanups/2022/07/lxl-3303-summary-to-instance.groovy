def where = """
  collection = 'bib' 
  AND data['@graph'][1]['instanceOf']['@type'] = '"Text"'
  AND data['@graph'][1]['instanceOf']['summary'] IS NOT NULL
  AND deleted = false
"""

def badShape = ['@type', 'label', 'language'] as Set

selectBySqlWhere(where) { bib ->
    if (bib.graph[0].descriptionCreator == ['@id':'https://libris.kb.se/library/APP1']) {
        return
    }

    def instance = bib.graph[1]
    def work = instance.instanceOf

    def normalizedSummary = work.summary.collect { Map s ->
        if (s.keySet().containsAll(badShape)) {
            def lang = s.subMap(['@type', 'language'])
            s.remove('language')
            return [lang, s]
        }
        s
    }.flatten()

    def (toWork, toInstance) = normalizedSummary.split { Map s ->
        s.containsKey('language')
    }

    if (toInstance) {
        if (toWork) {
            work['summary'] = toWork
        } else {
            work.remove('summary')
        }

        instance['summary'] = instance.summary ?: []
        toInstance.each { s ->
            if (!instance.summary.contains(s)) {
                instance.summary << s
            }
        }
        bib.scheduleSave()
    }
}

def asList(x) {
    (x ?: []).with {it instanceof List ? it : [it] }
}

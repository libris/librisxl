Set iaLabels = ['0-3 år', '3-6 år', '6-9 år', '9-12 år', '12-15 år', '15']

def where = """
collection = 'bib' 
and deleted = false
and data #>> '{@graph,1,instanceOf,genreForm}' is not null
"""

selectBySqlWhere(where) { bib ->
    def work = bib.graph[1].instanceOf
    def toIntendedAudience = []
    work.genreForm.removeAll { gf ->
        if (gf.prefLabel in iaLabels) {
            toIntendedAudience.add(gf.prefLabel)
        }
    }
    if (work.genreForm.isEmpty()) {
        work.remove('genreForm')
    }
    if (toIntendedAudience) {
        toIntendedAudience.each { label ->
            if (!asList(work.intendedAudience).any { ia -> ia.label == label }) {
                work['intendedAudience'] = asList(work.intendedAudience) + ['@type': 'IntendedAudience', 'label': label]
            }
        }
        bib.scheduleSave()
    }
}

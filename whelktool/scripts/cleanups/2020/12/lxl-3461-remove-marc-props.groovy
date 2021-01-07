/**
 * See LXL-3461 for more info.
 */

import whelk.util.DocumentUtil

properties = ["marc:hasCountryOfProducingEntityForArchivalFilms",
              "marc:languageNote",
              "marc:soundAspect",
              "marc:transposition"]

PrintWriter report = getReportWriter("report")

selectByCollection('bib') { rec ->
    def instance = rec.doc.data['@graph'][1]
    boolean removed = DocumentUtil.traverse(instance) { value, List path ->
        if (!path) {
            return
        }
        def key = path.last() as String
        if (properties.contains(key)) {
            report.println("${rec.doc.getShortId()}, $key")
            return new DocumentUtil.Remove()
        }
    }
    if (removed) {
        rec.scheduleSave()
    }
}

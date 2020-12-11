/**
 * See LXL-3461 for more info.
 */

import whelk.util.DocumentUtil

properties = ["marc:hasCountryOfProducingEntityForArchivalFilms",
              "marc:languageNote",
              "marc:soundAspect",
              "marc:transposition"]

selectByCollection('bib') { bib ->
    def instance = bib.doc.data['@graph'][1]
    boolean removed = DocumentUtil.traverse(instance) { value, List path ->
        if (!path) {
            return
        }
        def key = path.last() as String
        if (properties.contains(key)) {
            return new DocumentUtil.Remove()
        }
    }
    if (removed) {
        bib.scheduleSave()
    }
}

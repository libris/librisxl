/*

Examples of
rule 1:
[(Alexander den store), (i litteraturen)] => (Alexander den store), (Motiv i litteraturen)
rule 2:
[(Symboler) + (i konsten)] => (Symboler i konsten); the combination exists
rule 3:
[(Kommunikation) + (i konsten)] => (Kommunikation); the combination does not exist
rule 4:
The rest.
> 2 termComponents or combinations of subdivision and local entities.

See https://jira.kb.se/browse/LXL-3817 for details.

*/

import whelk.Document
import whelk.util.DocumentUtil

SUBDIVISION_TO_SUBJECT = [
        "https://id.kb.se/term/sao/i%20Bibeln"      : "https://id.kb.se/term/sao/Bibliska%20motiv",
        "https://id.kb.se/term/sao/i%20filmen"      : "https://id.kb.se/term/sao/Motiv%20i%20filmen",
        "https://id.kb.se/term/sao/i%20konsten"     : "https://id.kb.se/term/sao/Motiv%20i%20konsten",
        "https://id.kb.se/term/sao/i%20litteraturen": "https://id.kb.se/term/sao/Motiv%20i%20litteraturen",
        "https://id.kb.se/term/sao/i%20massmedia"   : "https://id.kb.se/term/sao/Motiv%20i%20massmedia",
        "https://id.kb.se/term/sao/i%20pressen"     : "https://id.kb.se/term/sao/Motiv%20i%20pressen",
]

selectByCollection('bib') { bib ->
    def data = bib.doc.data
    DocumentUtil.traverse(data, { value, path ->
        if (!(value instanceof Map && value.'@type'.equals("ComplexSubject") && value.termComponentList)) {
            return
        }

        def components = value.termComponentList
        def uriSubdivision = components.find { isSubdivision(it)}?.'@id'
        def uri = components.find { !isSubdivision(it) && it.'@id' }?.'@id'

        if (components.size() == 2 && uriSubdivision && uri) {
            boolean rule1 = false
            boolean rule2 = false
            boolean rule3 = false

            def prefLabelForSubdivision
            def prefLabelForSubject

            selectByIds([uriSubdivision]) { d ->
                Map thing = d.doc.data['@graph'][1]
                prefLabelForSubdivision = thing.prefLabel
            }

            selectByIds([uri]) { d ->
                Map thing = d.doc.data['@graph'][1]
                prefLabelForSubject = thing.prefLabel
                if (thing.'@type' in ["Person", "Geographic", "Organisation"]) {
                    rule1 = true
                }
            }

            def label = prefLabelForSubject + " " + prefLabelForSubdivision
            String combinedId = "https://id.kb.se/term/sao/" + URLEncoder.encode(label, 'UTF-8').replace("+", "%20")

            selectByIds([combinedId]) {
                rule2 = true
            }

            if (!rule2 && uri) {
                rule3 = true
            }

            def pathCopy = path.collect()
            pathCopy.removeLast()
            def subjectRoot = getAtPath(data, pathCopy)

            if (rule1) {
                incrementStats("Regel 1", bib.doc.id)
                subjectRoot.add(["@id" : SUBDIVISION_TO_SUBJECT[uriSubdivision]])
                bib.scheduleSave()
                return new DocumentUtil.Replace(["@id" : uri])
            } else if (rule2) {
                incrementStats("Regel 2", bib.doc.id)
                bib.scheduleSave()
                return new DocumentUtil.Replace(["@id" : combinedId])
            } else if (rule3) {
                incrementStats("Regel 3", bib.doc.id)
                bib.scheduleSave()
                return new DocumentUtil.Replace(["@id" : uri])
            }
        }
        else if (uriSubdivision && (components.size() > 2 || !uri)) {
                incrementStats("Regel 4", bib.doc.id)
        }
    })
}

private boolean isSubdivision(it) {
    it.'@id' in SUBDIVISION_TO_SUBJECT.keySet()
}

def getAtPath(data, List path) {
    return Document._get(path, data)
}

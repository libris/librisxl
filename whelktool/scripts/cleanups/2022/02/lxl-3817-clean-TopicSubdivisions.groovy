/*

Examples of
rule 1a:
[(Alexander den store), (i litteraturen)] => (Alexander den store), (Motiv i litteraturen)
rule 1b
[(Wessex (England) (lokal)), (i litteraturen)] => (Wessex (England) (olänkad)), (Motiv i litteraturen)
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

AGENTS = ["Person", "Geographic", "Organization", "Jurisdiction", "Meeting", "Family"]

//selectByIds(['1jb45wtc5rzvpw4']) { bib ->
selectByCollection('bib') { bib ->
    def data = bib.doc.data
    def subjectRoot = []
    DocumentUtil.traverse(data, { value, path ->
        if (!(value instanceof Map && value.'@type'.equals("ComplexSubject") && value.termComponentList)) {
            return
        }

        def components = value.termComponentList
        def uriSubdivision = components.find { isSubdivision(it)}?.'@id'
        def uri = components.find { !isSubdivision(it) && it.'@id' }?.'@id'
        def blankGeo = components.find { it.'@type'?.equals("Geographic") }

        if (components.size() == 2 && uriSubdivision && (uri || blankGeo)) {
            def prefLabelForSubdivision
            def prefLabelForSubject

            selectByIds([uriSubdivision]) { d ->
                Map thing = d.doc.data['@graph'][1]
                prefLabelForSubdivision = thing.prefLabel
            }

            def label = prefLabelForSubject + " " + prefLabelForSubdivision
            String combinedId = "https://id.kb.se/term/sao/" + URLEncoder.encode(label, 'UTF-8').replace("+", "%20")

            def pathCopy = path.collect()
            pathCopy.removeLast()
            subjectRoot = getAtPath(data, pathCopy)

            bib.scheduleSave()

            if (isRule1a(uri, prefLabelForSubject)) {
                incrementStats("Regel 1a", bib.doc.id)
                subjectRoot.add(["@id" : SUBDIVISION_TO_SUBJECT[uriSubdivision]])
                return new DocumentUtil.Replace(["@id" : uri])
            } else if (isRule1b(blankGeo)) {
                incrementStats("Regel 1b", bib.doc.id)
                subjectRoot.add(["@id" : SUBDIVISION_TO_SUBJECT[uriSubdivision]])
                return new DocumentUtil.Replace(blankGeo)
            } else if (isRule2(combinedId)) {
                incrementStats("Regel 2", bib.doc.id)
                return new DocumentUtil.Replace(["@id" : combinedId])
            } else if (isRule3(isRule2(combinedId), uri)) {
                incrementStats("Regel 3", bib.doc.id)
                return new DocumentUtil.Replace(["@id" : uri])
            }
        }
        else if (uriSubdivision && (components.size() > 2 || !uri)) {
                incrementStats("Regel 4", bib.doc.id)
        }
    })
    subjectRoot.unique()
}

private boolean isRule3(boolean rule2, uri) {
    return !rule2 && uri
}

private boolean isRule2(String combinedId) {
    def b = false
    selectByIds([combinedId]) {
        b = true
    }
    return b
}

private boolean isRule1b(blankGeo) {
    return blankGeo
}

private boolean isRule1a(uri, prefLabelForSubject) {
    def b = false
    selectByIds(uri ? [uri] : []) { d ->
        Map thing = d.doc.data['@graph'][1]
        prefLabelForSubject = thing.prefLabel
        if (thing.'@type' in AGENTS) {
            b = true
        }
    }
    return b
}

private boolean isSubdivision(it) {
    it.'@id' in SUBDIVISION_TO_SUBJECT.keySet()
}

def getAtPath(data, List path) {
    return Document._get(path, data)
}

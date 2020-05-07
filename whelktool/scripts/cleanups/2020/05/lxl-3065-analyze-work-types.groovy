/* For analysis */

report = getReportWriter("report")

selectByCollection('bib') { bib ->
    def work = getWork(bib)
    def recordId = bib.graph[0][ID]
    def sabClassifications = getAllSAB(work)
    def codes = sabClassifications.collect { getSABcode(it)}
    report.println("SAB codes: $codes Type: ${work[TYPE]} Record ID: $recordId")
}

private List getAllSAB(work) {
    def classif = work.classification
    classif = classif instanceof Map ? [classif] : classif
    return classif?.findAll { c -> isSAB(c) }
}

boolean isSAB(classification) {
    if (isInstanceOf(classification, 'Classification')) {
        def inSchemeCode = classification.inScheme?.code as String
        return inSchemeCode && inSchemeCode == "kssb" && isInstanceOf(classification.inScheme, 'ConceptScheme')
    }
    return false
}

def getSABcode(classification) {
    return classification?.code instanceof String ? [classification.code] : classification.code
}

Map getWork(def bib) {
    def (record, thing, work) = bib.graph
    if (thing && isInstanceOf(thing, 'Work')) {
        return thing
    }
    else if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    }
    else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}
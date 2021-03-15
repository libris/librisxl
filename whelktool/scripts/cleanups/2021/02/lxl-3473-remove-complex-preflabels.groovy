import whelk.Document;
import whelk.Whelk;

String where = """
    ( collection = 'bib' and data#>'{@graph,1,instanceOf,subject}' @> '[{"@type":"ComplexSubject"}]' )
    or
    ( collection = 'auth' and data#>'{@graph,1,subject}' @> '[{"@type":"ComplexSubject"}]' )
"""

selectBySqlWhere(where) { data ->
    def mainEntity = data.graph[1]
    def subjectEntities = []
    if (mainEntity["subject"])
    {
        subjectEntities.addAll ( asList(mainEntity["subject"]) )
    }
    if (mainEntity["instanceOf"])
    {
        if (mainEntity["instanceOf"]["subject"])
        {
            subjectEntities.addAll ( asList(mainEntity["instanceOf"]["subject"]) )
        }
    }

    def modified = false
    subjectEntities.each { subject ->
        if (subject &&
                subject["@type"] &&
                subject["@type"] == "ComplexSubject") {
            modified |= (subject.remove("prefLabel") != null)
            modified |= (subject.remove("sameAs") != null)
        }
    }

    if (modified)
        data.scheduleSave()
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}

unhandled = getReportWriter("UNHANDLED")
String where = "collection = 'bib' and data#>'{@graph,1,subject}' is not null"

selectBySqlWhere(where) { data ->
    if (! data.graph[1].subject instanceof List) {
        unhandled.println(data.doc.getId() + " wierd instance subjects: " + data.graph[1].subject)
        return
    }
    if (data.graph[1].instanceOf == null) {
        unhandled.println(data.doc.getId() + " missing instanceOf?")
        return
    }
    if (data.graph[1].instanceOf.keySet() == ["@id"] as Set) {
        unhandled.println(data.doc.getId() + " linked work")
        return
    }

    List malplacedSubjects = asList(data.graph[1].subject)
    List workSubjects = data.graph[1].instanceOf.subject
    if (workSubjects == null) {
        data.graph[1].instanceOf.put("subject", [])
        workSubjects = data.graph[1].instanceOf.subject
    }

    for (Map subject : malplacedSubjects) {
        if (!workSubjects.contains(subject))
            workSubjects.add(subject)
    }

    data.graph[1].remove("subject")
    data.scheduleSave()
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}

String where = """
    data#>'{@graph,1,instanceOf,subject}' LIKE '%https://id.kb.se/term/sao/Jaktskildringar%'
    AND collection = 'bib'
"""

selectBySqlWhere(where) { data ->
    asList(data.graph[1].instanceOf.subject).each { subject ->
        replaceLinks(subject,
                "https://id.kb.se/term/saogf/Jaktskildringar",
                "https://id.kb.se/term/sao/Jaktskildringar")
    }
}

void replaceLinks(Object node, String newLinkTarget, String linkToReplace) {
    if (node instanceof Map) {
        Map map = node

        if (map.size() == 1 && map["@id"] == linkToReplace ) {
            map["@id"] = newLinkTarget
        }

        for (String key : map.keySet()) {
            replaceLinks(map[key], newLinkTarget, linkToReplace)
        }
    }

    if (node instanceof List) {
        List list = node

        for (Object e : list) {
            replaceLinks(e, newLinkTarget, linkToReplace)
        }

        // Check that we've not created a double link (in case there was one before we started)
        boolean alreadyFound = false
        Iterator it = list.iterator()
        while (it.hasNext()) {
            Object e = it.next()
            if (e instanceof Map && e.size() == 1 && e["@id"] != null && e["@id"] == newLinkTarget) {
                if (alreadyFound) { it.remove() }
                else alreadyFound = true
            }
        }
    }
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}

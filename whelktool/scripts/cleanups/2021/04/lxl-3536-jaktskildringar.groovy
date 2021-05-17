String where = """
    data#>>'{@graph,1,instanceOf,subject}' LIKE '%https://id.kb.se/term/sao/Jaktskildringar%'
    AND collection = 'bib'
"""

selectBySqlWhere(where) { data ->
    replaceLinks( (Map) data.graph[1].instanceOf,
            "https://id.kb.se/term/saogf/Jaktskildringar",
            "https://id.kb.se/term/sao/Jaktskildringar")
    data.scheduleSave()
}

void replaceLinks(Map work, String newLinkTarget, String linkToReplace) {

    boolean containedBadLink = false

    // Remove bad links
    if (work.subject instanceof List) {
        Iterator it = work.subject.iterator()
        while (it.hasNext()) {
            Map subject = it.next()
            if (subject.size() == 1 && subject["@id"] == linkToReplace ) {
                it.remove()
                containedBadLink = true
            }
        }
    }
    else if (work.subject instanceof Map && work.subject.size() == 1 && work.subject["@id"] == linkToReplace) {
        work.remove("subject")
        containedBadLink = true
    }

    // If something was removed, instead set the new one.
    if (containedBadLink) {

        // Make sure genreForm is a list
        if (work.genreForm == null)
            work.put("genreForm", [])
        else if ( !(work.genreForm instanceof List) )
            work.put("genreForm", [work.genreForm])

        // Add the correct link
        work.genreForm.add(["@id" : newLinkTarget])

        // Check that we've not created a double link (in case there was one before we started)
        boolean alreadyFound = false
        Iterator it = work.genreForm.iterator()
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

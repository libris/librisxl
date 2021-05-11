String where = """
    collection = 'bib' 
    AND data#>'{@graph,1,instanceOf,hasNote}' @> '[{\"@type\":\"marc:ParticipantOrPerformerNote\"}]'
"""

selectBySqlWhere(where) { data ->
    Map work = data.graph[1].instanceOf
    List hasNote = work.hasNote
    List contribution = work.contribution

    List ppNotes = hasNote.findAll{
        it."@type" == "marc:ParticipantOrPerformerNote" && it.label
    }

    if (ppNotes.isEmpty())
        return

    boolean modified

    ppNotes.each { Map pp ->
        String label = asList(pp.label)[0] // Multiple labels occur (rarely), although not the kinds we bother with
        label = label.toLowerCase().replaceFirst(/ \p{L}\. /, " ") // Remove initial e.g. D.

        // E.g. "inläsare: ?", "skådespelare: --" or "medverkande:"
        if (label ==~ /^\p{L}+:((\P{L})+)?$/) {
            modified = hasNote.remove(pp)
            return
        }

        String relator

        if (label ==~ /^presenter.+/)
            relator = "https://id.kb.se/relator/onscreenPresenter"
        else if (label ==~ /^inläs(are|ning).+/)
            relator = "https://id.kb.se/relator/narrator"

        if (!relator)
            return

        boolean agentAndRoleInContribution = contribution.any { Map c ->
            Map agent = asList(c.agent)[0] // Never more than one agent
            String agentName = (agent?.givenName + " " + agent?.familyName).toLowerCase().replaceFirst(/ \p{L}\. /, " ")

            boolean roleIsSame = ["@id":relator] in asList(c.role)
            boolean agentIsSame = label.contains(agentName)
            boolean labelContainsMoreInfo = label.replaceAll(/^inläs(are|ning)|^presenter|$agentName/, "") ==~ /.*\p{L}.*/

            return roleIsSame && agentIsSame && !labelContainsMoreInfo
        }

        if (agentAndRoleInContribution)
            modified = hasNote.remove(pp)
    }

    if (hasNote.isEmpty())
        work.remove("hasNote")

    if (modified)
        data.scheduleSave()
}

List asList(Object o) {
    return o in List ? o : [o]
}
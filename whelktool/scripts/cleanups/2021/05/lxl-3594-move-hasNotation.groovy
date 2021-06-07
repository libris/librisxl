Map langs = Collections.synchronizedMap([:])
Map notations = Collections.synchronizedMap([:])

selectBySqlWhere("collection = 'definitions' AND data#>>'{@graph,1,@type}' = 'Language'") { data ->
    Map instance = data.graph[1]

    String mainEntityId = instance."@id"
    List prefLabels = instance.prefLabelByLang*.value

    langs[mainEntityId] = prefLabels
}

selectBySqlWhere("collection = 'definitions' AND data#>>'{@graph,1,@type}' ~ 'MusicNotation|TactileNotation'") { data ->
    Map instance = data.graph[1]

    String mainEntityId = instance."@id"
    List prefLabels = instance.prefLabelByLang*.value

    notations[mainEntityId] = prefLabels
}

String where = """
    collection = 'bib'
    AND data#>'{@graph,1,instanceOf,hasNote}' @> '[{\"@type\":\"marc:LanguageNote\"}]'
"""

selectBySqlWhere(where) { data ->
    Map work = data.graph[1].instanceOf
    List language = work.language
    List langNotes = work.hasNote.findAll { it."@type" == "marc:LanguageNote" && it.hasNotation }

    if (langNotes.isEmpty())
        return

    boolean modified

    langNotes.each { note ->
        if (!note.label) {
            work["hasNotation"] = note.hasNotation
            work.hasNote.remove(note)
            modified = true
        } else if (language.size() == 1) {
            String langUri = language[0]."@id"
            String prefLabels = langs[langUri].join("|")

            if (note.label ==~ /(?i)((in|på) )?($prefLabels)\P{L}*/) {
                work["hasNotation"] = note.hasNotation
                work.hasNote.remove(note)
                modified = true
            }
        }
    }

    if (work.hasNote.isEmpty())
        work.remove("hasNote")

    List notationsToLink = []

    // Remove linkable Notations
    work.hasNotation?.removeAll { n ->
        List label = asList(n.label)

        // Remove label(s) that can be linked to a Notation uri
        boolean link = label.removeAll { l ->
            String matchedNotationUri = notations.find {uri, prefLabels ->
                prefLabels = prefLabels.join("|")
                l ==~ /(?i)($prefLabels)\P{L}*/
            }?.key

            if (matchedNotationUri) {
                notationsToLink << matchedNotationUri
                return true
            }

            return false
        }

        // Remove the Notation object if the label(s) is linkable
        if (link && label.isEmpty())
            return true

        return false
    }

    // Add linked Notations
    if (notationsToLink)
        work.hasNotation += notationsToLink

    if (modified)
        data.scheduleSave()
}

List asList(o) {
    return o in List ? o : o != null ? [o] : []
}
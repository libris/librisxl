Map langs = Collections.synchronizedMap([:])

selectBySqlWhere("collection = 'definitions' AND data#>>'{@graph,1,@type}' = 'Language'") { data ->
    Map instance = data.graph[1]

    String mainEntityId = instance."@id"
    List prefLabels = instance.prefLabelByLang*.value

    langs[mainEntityId] = prefLabels
}

String where = """
    collection = 'bib'
    AND data#>>'{@graph,1,instanceOf,@type}' = 'Text'
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
        note.hasNotation = note.hasNotation.collect { n ->
            String label = n.label in List ? n.label[0] : n.label

            if (label ==~ /Traditionell västerländsk notskrift|Staff notation\.?/) {
                modified = true
                return ["@id": "https://id.kb.se/term/rda/musnotation/StaffNotation"]
            }
            //TODO: Link more notations such as "Latinsk skrift", "Kyrillisk skrift"?
            else {
                return n
            }
        }
        if (!note.label) {
            work["hasNotation"] = note.hasNotation
            work.hasNote.remove(note)
            modified = true
        } else if (language.size() == 1) {
            String langUri = language[0]."@id"
            String prefLabels = langs[langUri].join("|")

            if (note.label ==~ /(?i)(in|(text)?(på)?) ($prefLabels)\P{L}*/) {
                work["hasNotation"] = note.hasNotation
                work.hasNote.remove(note)
                modified = true
            }
        }
    }

    if (work.hasNote.isEmpty())
        work.remove("hasNote")

    if (modified)
        data.scheduleSave()
}
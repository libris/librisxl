File holdIDs = new File(scriptDir, "Li-pliktex-holdID.txt")

selectByIds( holdIDs.readLines() ) { hold ->
                 
        def items = hold.graph[1]

        if (items.hasNote) {
            List notes = items.hasNote
            notes.removeAll { note ->
                    // The label is either a string or a list of strings
                note.label instanceof String && note.label.contains('Pliktex') ||
                note.label instanceof List && note.label.any { it.contains('Pliktex') }
            }

            if (notes.isEmpty()) {
                items.remove('hasNote')
            }
        }
               
        def hasComponent = hold.graph[1]['hasComponent']

        hasComponent?.each { c ->
        if (c['hasNote'] instanceof List) {
            def removed = c['hasNote'].removeAll { n ->
                asList(n['label']).any { it.contains('Pliktex') }
            }
            if (c['hasNote'].isEmpty()) {
                c.remove('hasNote')
            }
        } else if (c['hasNote'] instanceof Map && asList(c['hasNote']['label']).any { it.contains('Pliktex') }) {
            c.remove('hasNote')
            }
        }
        
        if (items["immediateAcquisition"]) {
                items["immediateAcquisition"] <<
                [
                    "@type": "ImmediateAcquisition",
                    "marc:sourceOfAcquisition": "Pliktex."
                ]
        }
        if (!items["immediateAcquisition"]) {
                items["immediateAcquisition"] = []
                items["immediateAcquisition"] <<
                [
                    "@type": "ImmediateAcquisition",
                    "marc:sourceOfAcquisition": "Pliktex."
                ]
        }
               
        hold.scheduleSave(loud: true)
}

List asList(o) {
    return o in List ? o : o != null ? [o] : []
}
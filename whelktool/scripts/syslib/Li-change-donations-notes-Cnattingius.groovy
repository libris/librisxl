def where = """id in
    (select lb.id
    from lddb lb
    where lb.collection = 'hold' and
    lb.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Li'
    and lb.data::text ILIKE '%Donation: Cnattingius%'
    and lb.data#>>'{@graph,1,custodialHistory}' IS NULL
    and lb.deleted = 'false')"""

selectBySqlWhere(where, silent: false, { hold ->

    def items = hold.graph[1]
    boolean modified = false

    if (items.hasNote) {
        List notes = items.hasNote
        notes.removeAll { note ->
            // The label is either a string or a list of strings
            note.label instanceof String && note.label.contains('Donation: Cnattingius') ||
                    note.label instanceof List && note.label.any { it.contains('Donation: Cnattingius') }
            modified = true
        }

        if (notes.isEmpty()) {
            items.remove('hasNote')
        }
    }

    if (items.cataloguersNote) {
        List catNotes = items.cataloguersNote
        catNotes.removeAll { catnote ->
            catnote instanceof String && catnote.contains('Donation: Cnattingius') ||
                    catnote instanceof List && catnote.any { it.contains('Donation: Cnattingius') }
            modified = true
        }

        if (catNotes.isEmpty()) {
            items.remove('cataloguersNote')
        }
    }

    def hasComponent = hold.graph[1]['hasComponent']

    hasComponent?.each { c ->
        if (c['hasNote'] instanceof List) {
            def removedNote = c['hasNote'].removeAll { n ->
                asList(n['label']).any { it.contains('Donation: Cnattingius') }
                modified = true
            }
            if (c['hasNote'].isEmpty()) {
                c.remove('hasNote')
            }
        } else if (c['hasNote'] instanceof Map && asList(c['hasNote']['label']).any { it.contains('Donation: Cnattingius') }) {
            c.remove('hasNote')
            modified = true
        }
        if (c['cataloguersNote']) {
            List HCcatNotes = c.cataloguersNote
            HCcatNotes.removeAll { catnote2 ->
                catnote2 instanceof String && catnote2.contains('Donation: Cnattingius') ||
                        catnote2 instanceof List && catnote2.any { it.contains('Donation: Cnattingius') }
                modified = true
            }

            if (HCcatNotes.isEmpty()) {
                c.remove('cataloguersNote')
            }
        }
        if (c.size() == 3 && c.containsKey('marc:groupid') && c.containsKey('heldBy') && c.containsKey('@type')) {
            Map component = c
            component.removeAll { it }
        }
        else if (c.size() == 2 && c.containsKey('heldBy') && c.containsKey('@type')) {
            Map component = c
            component.removeAll { it }
        }
    }

    hasComponent?.removeAll { !it }

    def hasTextualHoldings = hold.graph[1]['marc:hasTextualHoldingsBasicBibliographicUnit']

    hasTextualHoldings?.each { f ->
        if (f['marc:cataloguersNote']) {
            List THcatNotes = f['marc:cataloguersNote']
            THcatNotes.removeAll { thcatnote ->
                thcatnote instanceof String && thcatnote.contains('Donation: Cnattingius') ||
                        thcatnote instanceof List && thcatnote.any { it.contains('Donation: Cnattingius') }
                modified = true
            }

            if (THcatNotes.isEmpty()) {
                f.remove('marc:cataloguersNote')
            }
        }
        if (f['marc:publicNote']) {
            List pubcatNotes = f['marc:publicNote']
            pubcatNotes.removeAll { pubcatnote ->
                pubcatnote instanceof String && pubcatnote.contains('Donation: Cnattingius') ||
                        pubcatnote instanceof List && pubcatnote.any { it.contains('Donation: Cnattingius') }
                modified = true
            }

            if (pubcatNotes.isEmpty()) {
                f.remove('marc:publicNote')
            }
        }
        if (f.size() == 3 && f.containsKey('marc:typeOfNotation') && f.containsKey('marc:holdingsLevel') && f.containsKey('@type')) {
            Map component = f
            component.removeAll { it }
        }
        else if (f.size() == 4 && f.containsKey('marc:typeOfNotation') && f.containsKey('marc:holdingsLevel') && f.containsKey('@type') && f.containsKey('marc:groupid')){
            Map component = f
            component.removeAll { it }
        }
    }

    hasTextualHoldings?.removeAll { !it }

    if (!items["custodialHistory"]) {
        items["custodialHistory"] = 'Donation: Cnattingius'
        modified = true
    }

    if (modified) {
        hold.scheduleSave(loud: true)
    }
})
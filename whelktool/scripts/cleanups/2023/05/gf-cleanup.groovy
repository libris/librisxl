def substitutions =
        [
                (['@type': 'GenreForm', 'prefLabel': 'Romaner'])                                                                                                                                              : 'https://id.kb.se/term/saogf/Romaner',
                (['@type': 'GenreForm', 'prefLabel': 'Barn & Ungdom'])                                                                                                                                        : 'https://id.kb.se/term/barngf/Barn-%20och%20ungdomslitteratur',
                (['@type': 'GenreForm', 'prefLabel': 'Romantik'])                                                                                                                                             : 'https://id.kb.se/term/saogf/K%C3%A4rleksskildringar',
                (['@type': 'GenreForm', 'sameAs': [['@id': 'https://id.kb.se/term/barn/Mellan%C3%A5ldersb%C3%B6cker']], 'inScheme': ['@id': 'https://id.kb.se/term/barn'], 'prefLabel': 'Mellanåldersböcker']): 'https://id.kb.se/term/barngf/Mellan%C3%A5ldersb%C3%B6cker',
                (['@type': 'GenreForm', 'prefLabel': 'Lyrik'])                                                                                                                                                : 'https://id.kb.se/term/saogf/Poesi',
                (['@type': 'GenreForm', 'prefLabel': 'Noveller'])                                                                                                                                             : 'https://id.kb.se/term/saogf/Noveller',
                (['@type': 'GenreForm', 'prefLabel': 'Feelgood'])                                                                                                                                             : 'https://id.kb.se/term/saogf/Feelgood',
                (['@type': 'GenreForm', 'prefLabel': 'Unga vuxna'])                                                                                                                                           : 'https://id.kb.se/term/barngf/Unga%20vuxna',
                (['@type': 'GenreForm', 'prefLabel': 'Bilderböcker'])                                                                                                                                         : 'https://id.kb.se/term/barngf/Bilderb%C3%B6cker',
                (['@type': 'GenreForm', 'sameAs': [['@id': 'https://id.kb.se/term/barn/%C3%84ventyrsb%C3%B6cker']], 'inScheme': ['@id': 'https://id.kb.se/term/barn'], 'prefLabel': 'Äventyrsböcker'])        : 'https://id.kb.se/term/barngf/%C3%84ventyrsb%C3%B6cker'
        ]

selectByCollection('bib') {
    def instance = it.graph[1]
    def work = instance.instanceOf
    if (!(work instanceof Map)) {
        return
    }

    def isLargePrint = asList(instance.carrierType).any { it['@id'] == "https://id.kb.se/marc/LargePrint" }

    def modified = false

    work.genreForm?.removeAll { Map gf ->
        def linkableTerm = substitutions[gf]
        if (linkableTerm) {
            incrementStats(linkableTerm, gf.toString())
            gf.clear()
            gf['@id'] = linkableTerm
            modified = true
        }
        if (isLargePrint && gf == ['@type':'GenreForm', 'prefLabel':'Storstilsbok']) {
            incrementStats('removed from genreForm', gf.toString())
            return modified = true
        }
        return false
    }

    work.subject?.removeAll { s ->
        if (isLargePrint && s == ['@type':'Topic', 'label':['Storstil']]) {
            incrementStats('removed from subject', s.toString())
            return modified = true
        }
        return false
    }

    if (modified) {
        it.scheduleSave()
    }
}
/**
 * For all bibliographic records with SAB code "Dokb" (or a variant like "Dokb=foo", "Dokb/LC", "Dokb(p)",
 * this script ensures that they
 * - have genreForm "Personlig utveckling" (3gxp6ws01bjvn3qr)
 * - *don't* have subject "Personlig utveckling" (42gjhgln1gbxsdc)
 */

String GF_PERSONLIG_UTVECKLING = 'https://id.kb.se/term/saogf/Personlig%20utveckling'

String where = """
        collection = 'bib' AND
        deleted = false AND
        data#>>'{@graph,1,instanceOf,classification}' LIKE '%"Dokb%'
"""

selectBySqlWhere(where) { data ->
    boolean modified = false
    def thing = data.graph[1]

    // Make sure we've got a SAB Dokb (or variant) thing
    if (!(thing.instanceOf?.classification?.any {
        isInstanceOf(it, 'Classification') && it.code =~ /^(Dokb$|^Dokb=|Dokb\/|Dokb\()/
    })) {
        return
    }

    // Make sure we don't modify any possibly erroneously classified youth stuff
    if (thing.instanceOf?.intendedAudience?.any { it['@id'] == 'https://id.kb.se/marc/Juvenile' }) {
        return
    }

    if (!thing.instanceOf?.genreForm?.any { it['@id'] == GF_PERSONLIG_UTVECKLING }) {
        if (!thing.instanceOf.genreForm) {
            thing.instanceOf.genreForm = []
        }
        thing.instanceOf.genreForm << ['@id': GF_PERSONLIG_UTVECKLING]
        modified = true
    }

    thing.instanceOf?.subject?.removeAll {
        if (it['@id'] == 'https://id.kb.se/term/sao/Personlig%20utveckling') {
            modified = true
        }
    }

    if (modified) {
        data.scheduleSave();
    }
}

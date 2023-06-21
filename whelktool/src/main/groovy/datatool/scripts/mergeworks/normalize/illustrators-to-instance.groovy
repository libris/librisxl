import datatool.scripts.mergeworks.Util.Relator

import whelk.Whelk
import static whelk.JsonLd.ID_KEY

def ids = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }.flatten()

selectByIds(ids) { bib ->
    Map instance = bib.graph[1]
    def work = instance.instanceOf
    def contribution = work?.contribution

    if (!contribution) return

    def ill = [(ID_KEY): Relator.ILLUSTRATOR.iri]

    def modified = false

    contribution.removeAll { c ->
        if (!asList(c.role).contains(ill)) return false
        def has9pu = [(ID_KEY): Relator.PRIMARY_RIGHTS_HOLDER.iri] in asList(c.role)
        if (has9pu || isPictureBook(work) || isComics(work, bib.whelk)) return false
        instance['contribution'] = asList(instance['contribution']) + c.clone().tap { it['role'] = [ill] }
        c['role'] = asList(c.role) - ill
        modified = true
        return c.role.isEmpty()
    }

    if (contribution.isEmpty()) {
        work.remove('contribution')
    }

    if (modified) {
        bib.scheduleSave()
    }
}

boolean isPictureBook(Map work) {
    def picBookTerms = [
            'https://id.kb.se/term/barngf/Bilderb%C3%B6cker',
            'https://id.kb.se/term/barngf/Sm%C3%A5barnsbilderb%C3%B6cker'
    ].collect { [(ID_KEY): it] }

    return asList(work.genreForm).any { it in picBookTerms }
}

boolean isComics(Map work, Whelk whelk) {
    def comicsTerms = [
            'https://id.kb.se/term/saogf/Tecknade%20serier',
            'https://id.kb.se/term/barngf/Tecknade%20serier',
            'https://id.kb.se/term/gmgpc/swe/Tecknade%20serier',
            'https://id.kb.se/marc/ComicOrGraphicNovel',
            'https://id.kb.se/marc/ComicStrip'
    ].collect { [(ID_KEY): it] }

    return asList(work.genreForm).any {
        it in comicsTerms
                || it[ID_KEY] && whelk.relations.isImpliedBy('https://id.kb.se/term/saogf/Tecknade%20serier', it[ID_KEY])
                || asList(work.classification).any { it.code?.startsWith('Hci') }
    }
}
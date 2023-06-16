import datatool.scripts.mergeworks.Util.Relator

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
        if (has9pu || isPictureBook(work) || isComics(work)) return false
        instance['contribution'] = asList(instance['contribution']) + c.clone().tap { it['role'] = [ill] }
        println(instance['contribution'])
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
    [(ID_KEY): 'https://id.kb.se/term/barngf/Bilderb%C3%B6cker'] in asList(work.genreForm)
}

boolean isComics(Map work) {
    asList(work.genreForm).any { it[ID_KEY] ==~ 'https://id.kb.se/term/(saogf|barngf)/Tecknade%20serier' }
            || asList(work.classification).any { it.code?.startsWith('Hci') }
}
import whelk.Whelk

import static se.kb.libris.mergeworks.Util.Relator
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY

report = getReportWriter('report.tsv')

def ids = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }.flatten()

def whelk = getWhelk()
def instanceRolesByDomain = whelk.resourceCache.relators.findResults {
    if (it.domain) {
        def domain = whelk.jsonld.toTermKey(it.domain[ID_KEY])
        if (whelk.jsonld.isSubClassOf(domain, 'Embodiment')) it.subMap([ID_KEY])
    }
}
def instanceRoles = instanceRolesByDomain + [Relator.ILLUSTRATOR, Relator.AUTHOR_OF_INTRO, Relator.AUTHOR_OF_AFTERWORD].collect { [(ID_KEY): it.iri] }

selectByIds(ids) { bib ->
    Map instance = bib.graph[1]
    def work = instance.instanceOf
    def contribution = work?.contribution

    if (!contribution) return

    def ill = [(ID_KEY): Relator.ILLUSTRATOR.iri]

    def modified = false

    contribution.removeAll { c ->
        if (isPrimaryContribution(c)) return false

        def toInstance = asList(c.role).intersect(instanceRoles)
        if (toInstance.contains(ill)) {
            if (has9pu(c) || isPictureBook(work) || isComics(work, bib.whelk) || isStillImage(work)) {
                toInstance.remove(ill)
            }
        }
        if (toInstance) {
            instance['contribution'] = asList(instance['contribution']) + c.clone().tap { it['role'] = toInstance }
            c['role'] = asList(c.role) - toInstance
            modified = true
            report.println([bib.doc.shortId, toInstance.collect { it[ID_KEY].split('/').last() }].join('\t'))
            incrementStats('moved to instance', toInstance)
            return c.role.isEmpty()
        }

        return false
    }

    if (contribution.isEmpty()) {
        work.remove('contribution')
    }

    if (modified) {
        bib.scheduleSave()
    }
}

boolean isPrimaryContribution(Map contribution) {
    contribution[TYPE_KEY] == 'PrimaryContribution'
}

boolean has9pu(Map contribution) {
    asList(contribution.role).contains([(ID_KEY): Relator.PRIMARY_RIGHTS_HOLDER.iri])
}

boolean isStillImage(Map work) {
    asList(work.contentType).contains([(ID_KEY): 'https://id.kb.se/term/rda/StillImage'])
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
/**
 * Move contribution to instance if the role's domain is (or is subclass of) Embodiment.
 * Also move illustrator to instance if none of the following criteria is met:
 *  - The illustrator is the primary contributor (PrimaryContribution)
 *  - Classification indicates a picture book or comics
 *  - Genre/form indicates a picture book or comics
 * See isComics() and isPictureBook() below for details.
 */

import whelk.Whelk

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

import static se.kb.libris.mergeworks.Util.Relator
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY

report = getReportWriter('report.tsv')

def clusters = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }

def whelk = getWhelk()
def instanceRolesByDomain = whelk.resourceCache.relatorResources.relators.findResults {
    if (it.domain) {
        def domain = whelk.jsonld.toTermKey(it.domain[ID_KEY])
        if (whelk.jsonld.isSubClassOf(domain, 'Embodiment')) it.subMap([ID_KEY])
    }
}

def ill = [(ID_KEY): Relator.ILLUSTRATOR.iri]
def ninePu = [(ID_KEY): Relator.PRIMARY_RIGHTS_HOLDER.iri]
def instanceRoles = instanceRolesByDomain + [ill, ninePu]

def keepIllustratorOnWorkForIds = [:]

clusters.each { c ->
    def keepOnWork = new ConcurrentHashMap<Map, ConcurrentLinkedQueue>()
    def noIndicationOfKeeping = new ConcurrentHashMap<Map, ConcurrentLinkedQueue>()

    selectByIds(c) { bib ->
        def id = bib.doc.shortId
        Map instance = bib.graph[1]
        Map work = instance.instanceOf
        work?.contribution?.each { contrib ->
            if (asList(contrib.role).contains(ill)) {
                def agent = asList(contrib.agent).find()
                if (!agent) return
                if (isPrimaryContribution(contrib)
                        || isPictureBook(work)
                        || isComics(work, bib.whelk)
                ) {
                    keepOnWork.computeIfAbsent(agent, f -> new ConcurrentLinkedQueue()).add(id)
                } else {
                    noIndicationOfKeeping.computeIfAbsent(agent, f -> new ConcurrentLinkedQueue()).add(id)
                }
            }
        }
    }

    keepOnWork.each { agent, ids ->
        keepIllustratorOnWorkForIds.computeIfAbsent(agent, f -> [] as Set).with { s ->
            s.addAll(ids)
            if (noIndicationOfKeeping[agent]) {
                s.addAll(noIndicationOfKeeping[agent])
            }
        }
    }
}

selectByIds(clusters.flatten()) { bib ->
    def id = bib.doc.shortId
    Map instance = bib.graph[1]
    def work = instance.instanceOf
    def contribution = work?.contribution

    if (!contribution) return

    def modified = false

    contribution.removeAll { c ->
        if (isPrimaryContribution(c)) return false

        def toInstance = asList(c.role).intersect(instanceRoles)
        if (toInstance.contains(ill)) {
            def illustrator = asList(c.agent).find()
            if (!illustrator) return
            if (id in keepIllustratorOnWorkForIds[illustrator]) {
                toInstance.remove(ill)
                toInstance.remove(ninePu)
            }
        } else if (toInstance.contains(ninePu)) {
            toInstance.remove(ninePu)
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

boolean isPictureBook(Map work) {
    def picBookTerms = [
            'https://id.kb.se/term/barngf/Bilderb%C3%B6cker',
            'https://id.kb.se/term/barngf/Sm%C3%A5barnsbilderb%C3%B6cker',
            'https://id.kb.se/term/barngf/Pekb%C3%B6cker'
    ].collect { [(ID_KEY): it] }

    return asList(work.genreForm).any { it in picBookTerms } || asList(work.classification).any { it.code == 'Hcf(yb)' }
}

boolean isComics(Map work, Whelk whelk) {
    def comicsTerms = [
            'https://id.kb.se/term/saogf/Tecknade%20serier',
            'https://id.kb.se/term/barngf/Tecknade%20serier',
            'https://id.kb.se/term/gmgpc/swe/Tecknade%20serier',
            'https://id.kb.se/marc/ComicOrGraphicNovel',
            'https://id.kb.se/marc/ComicStrip',
            'https://id.kb.se/term/barngf/Bildromaner',
            'https://id.kb.se/term/barngf/Manga'
    ].collect { [(ID_KEY): it] }

    return asList(work.genreForm).any {
        it in comicsTerms
                || it[ID_KEY] && whelk.relations.isImpliedBy('https://id.kb.se/term/saogf/Tecknade%20serier', it[ID_KEY])
                || asList(work.classification).any { it.code?.startsWith('Hci') }
    }
}
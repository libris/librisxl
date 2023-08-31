import whelk.Whelk

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

import static se.kb.libris.mergeworks.Util.Relator
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY

report = getReportWriter('report.tsv')
//mixed = getReportWriter('mixed.tsv')
//keep = getReportWriter('keep.tsv')
//moveFor = getReportWriter('move.tsv')

def clusters = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }

def whelk = getWhelk()
def instanceRolesByDomain = whelk.resourceCache.relators.findResults {
    if (it.domain) {
        def domain = whelk.jsonld.toTermKey(it.domain[ID_KEY])
        if (whelk.jsonld.isSubClassOf(domain, 'Embodiment')) it.subMap([ID_KEY])
    }
}
def instanceRoles = instanceRolesByDomain + [Relator.ILLUSTRATOR, Relator.AUTHOR_OF_INTRO, Relator.AUTHOR_OF_AFTERWORD].collect { [(ID_KEY): it.iri] }
def ill = [(ID_KEY): Relator.ILLUSTRATOR.iri]

def keepIllustratorOnWorkForIds = [:]

clusters.each { c ->
    def keepOnWork = new ConcurrentHashMap<Map, ConcurrentLinkedQueue>()
    def electronic = new ConcurrentHashMap<Map, ConcurrentLinkedQueue>()
//    def move = new ConcurrentLinkedQueue()

    selectByIds(c) { bib ->
        def id = bib.doc.shortId
        Map instance = bib.graph[1]
        Map work = instance.instanceOf
        work.contribution?.each { contrib ->
            if (asList(contrib.role).contains(ill)) {
                def agent = asList(contrib.agent).find()
                if (!agent) return
                if (isPrimaryContribution(contrib)
                        || has9pu(contrib)
                        || isPictureBook(work)
                        || isComics(work, bib.whelk)
                        || isStillImage(work)
                ) {
                    keepOnWork.computeIfAbsent(agent, f -> new ConcurrentLinkedQueue()).add(id)
                } else if (instance[TYPE_KEY] == 'Electronic') {
                    electronic.computeIfAbsent(agent, f -> new ConcurrentLinkedQueue()).add(id)
                }
//                else {
//                    move.add(id)
//                }
            }
        }
    }

    keepOnWork.each { agent, ids ->
        keepIllustratorOnWorkForIds.computeIfAbsent(agent, f -> [] as Set).with { s ->
            s.addAll(ids)
            if (electronic[agent]) {
                s.addAll(electronic[agent])
            }
        }
    }

//    if (keepOnWork && move) {
//        mixed.println(c.join('\t'))
//    } else if (keepOnWork) {
//        keep.println(c.join('\t'))
//    } else if (move) {
//        moveFor.println(c.join('\t'))
//    }
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
            'https://id.kb.se/term/barngf/Sm%C3%A5barnsbilderb%C3%B6cker',
            'https://id.kb.se/term/barngf/Pekb%C3%B6cker'
    ].collect { [(ID_KEY): it] }

    return asList(work.genreForm).any { it in picBookTerms }
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
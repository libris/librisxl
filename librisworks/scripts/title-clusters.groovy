/**
 * Partition each cluster into smaller clusters based on strict title matching.
 */

import se.kb.libris.mergeworks.Doc

import static se.kb.libris.mergeworks.Util.partition

new File(System.getProperty('clusters')).splitEachLine(~/[\t ]+/) {cluster ->
    List<Doc> docs = Collections.synchronizedList([])
    selectByIds(cluster) {
        docs.add(new Doc(it))
    }
    titleClusters(docs).findAll { it.size() > 1 }.each {
        println(it.collect { it.shortId() }.join('\t'))
    }
}

Collection<Collection<Doc>> titleClusters(Collection<Doc> docs) {
    return partitionByTitle(docs)
            .findAll { !it.any { doc -> doc.hasGenericTitle() } }
            // Replace instances sharing the same linked work with only the linked work
            .collect { loadUniqueLinkedWorks(it) + it.findAll {d -> !d.workIri() } }
            .findAll { it.size() > 1 }
            .sort { a, b -> a.first().view.instanceDisplayTitle() <=> b.first().view.instanceDisplayTitle() }
}

static Collection<Collection<Doc>> partitionByTitle(Collection<Doc> docs) {
    return partition(docs) { Doc a, Doc b ->
        def aTitles = a.flatInstanceTitle() + a.flatWorkTitle()
        def bTitles = b.flatInstanceTitle() + b.flatWorkTitle()
        !aTitles.intersect(bTitles).isEmpty()
    }
}

Collection<Doc> loadUniqueLinkedWorks(Collection<Doc> docs) {
    def uniqueWorkIds = docs.findResults { it.workIri() }.unique()
    def uniqueWorkDocs = Collections.synchronizedList([])
    if (uniqueWorkIds) {
        selectByIds(uniqueWorkIds) {
            uniqueWorkDocs.add(new Doc(it))
        }
    }
    return uniqueWorkDocs
}
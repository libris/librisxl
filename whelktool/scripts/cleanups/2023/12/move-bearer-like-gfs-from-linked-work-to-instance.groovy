/**
 * LXL-4418
 * Move bearer-like genre/form terms from work to instance like in
 * ../../2020/08/lxl-3294-move-bearer-like-gfs-from-work-to-instance.groovy,
 * however in this script we do it retroactively for works that have already been linked,
 * i.e. the given genre/form term is removed from the linked work and instead put in all
 * instances (of that linked work) that had the term in it's local work before.
 */

removedFromLinkedWork = getReportWriter("removed-from-linked-work.txt")
addedToInstance = getReportWriter("added-to-instance.txt")

gfsToMove = [
        "E-böcker",
        "E-single",
        "E-textbok",
        "Electronic books",
        "Ljudbok cd",
        "Ljudbok kassett",
        "Ljudbok mp3",
        "Musik cd",
        "Tal cd",
        "Talbok Daisy",
        "Talbok kassett",
        "Video dvd",
        "Video vhs",
        "Historiska faksimil",
        "Deckare & spänning",
        "Fantasy & SF",
        "Idrott & friluftsaktiviteter",
        "Hus & hem",
        "Medicin & hälsa",
        "Resor & geografi",
        "Samhälle & politik"
] as Set

modifiedWorks = Collections.synchronizedSet([] as Set)

selectBySqlWhere("collection = 'auth' and deleted = false and data#>>'{@graph,1,@type}' = 'Text'") { auth ->
    try {
        removeGfFromWork(auth)
    }
    catch (Exception e) {
        System.err.println("${auth.doc.shortId} $e")
        e.printStackTrace()
    }
}

selectBySqlWhere("collection = 'bib' and deleted = false and data#>'{@graph,1,instanceOf,@id}' is not null") { bib ->
    try {
        addGfToInstance(bib)
    }
    catch (Exception e) {
        System.err.println("${bib.doc.shortId} $e")
        e.printStackTrace()
    }
}

void removeGfFromWork(auth) {
    def work = auth.graph[1]

    if (!work.genreForm) return

    def gf = work.genreForm

    assert gf instanceof List

    def removed = gf.removeAll {
        return it.prefLabel in gfsToMove
    }

    if (removed) {
        if (gf.isEmpty()) work.remove('genreForm')
        auth.scheduleSave()
        modifiedWorks.add(work['@id'])
        removedFromLinkedWork.println(auth.doc.shortId)
    }
}

void addGfToInstance(bib) {
    def instance = bib.graph[1]

    if (!modifiedWorks.contains(instance['instanceOf']['@id'])) return

    def newestToOldestVersion = bib.getVersions().reverse()
    def versionBeforeLinked = newestToOldestVersion.find { !it.data['@graph'][1]['instanceOf'].containsKey('@id') }

    if (!versionBeforeLinked) return

    def addThese = asList(versionBeforeLinked.data['@graph'][1]['instanceOf']['genreForm']).collect { it.prefLabel in gfsToMove }

    if (addThese) {
        instance['genreForm'] = (asList(instance['genreForm']) + addThese).unique()
        bib.scheduleSave()
        addedToInstance.println(bib.doc.shortId)
    }
}
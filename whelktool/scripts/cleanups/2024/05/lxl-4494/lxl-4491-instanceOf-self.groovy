/**
 Follow-up for LXL-4491
 Fix documents where instanceOf has been replaced with a link to itself
 */
noGraph2 = getReportWriter("noGraph2")


selectByIds(new File(scriptDir, "ids.txt").readLines()) { bib ->
    def (record, instance) = bib.graph
    if (instance['@id'] == instance['instanceOf']['@id']) {
        if (bib.graph.size() == 3) {
            def work = bib.graph[2]
            work.remove('@id')
            instance['instanceOf'] = work
            bib.doc.data['@graph'] = [record, instance]
        } else {
            noGraph2.println(bib.doc.shortId)
            var newestToOldest = getWhelk().storage.loadAllVersions(bib.doc.shortId).reversed()
            var notBroken = newestToOldest.find { getAtPath(it.data, ['@graph',1,'@id']) != getAtPath(it.data, ['@graph',1,'instanceOf','@id']) }
            instance['instanceOf'] = notBroken.data['@graph'][1]['instanceOf']
        }
        bib.scheduleSave(loud: true)
    }
}
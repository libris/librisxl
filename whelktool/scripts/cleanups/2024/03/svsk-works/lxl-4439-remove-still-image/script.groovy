import static whelk.JsonLd.isLink

STILL_IMAGE = ["@id": "https://id.kb.se/term/rda/StillImage"]

Set handledWorks = Collections.synchronizedSet([] as Set)

selectByIds(new File(scriptDir, 'MODIFIED.txt').readLines()) { docItem ->
    def instance = docItem.graph[1]
    def work = instance.instanceOf

    if (isLink(work)) {
        def workId = work['@id']
        if (handledWorks.add(workId)) {
            selectByIds([workId]) {
                def linkedWork = it.graph[1]
                if (hasStillImage(linkedWork) && !hasIllustrator(linkedWork)) {
                    linkedWork['contentType'].remove(STILL_IMAGE)
                    it.scheduleSave()
                }
            }
        }
    } else {
        if (hasStillImage(work) && !hasIllustrator(work)) {
            work['contentType'].remove(STILL_IMAGE)
            docItem.scheduleSave()
        }
    }
}

boolean hasIllustrator(Map work) {
    asList(work['contribution']).any {
        asList(it['role']).any { it['@id'] == 'https://id.kb.se/relator/illustrator' }
    }
}

boolean hasStillImage(Map work) {
    asList(work['contentType']).any { it == STILL_IMAGE }
}

/**
 *
 *
 *
 * See https://kbse.atlassian.net/browse/TG-50
 *
 */


import whelk.JsonLd
import whelk.history.History
import whelk.util.DocumentUtil
import whelk.util.Unicode

byPath = getReportWriter("changes-by-path.txt")
byVersion = getReportWriter("changes-by-version.txt")
errors = getReportWriter("errors.txt")

whelk = getWhelk()

//selectByIds(['bvnpzmmn4qf7nzs']) {
selectByCollection('bib') {
    process(it)
}

void process(bib) {
    try {
        var shortId = bib.doc.getShortId()
        var docVersions = whelk.storage.loadDocumentHistory(shortId)
        History history = new History(docVersions, whelk.jsonld)
        List changeSets = history.m_changeSetsMap.changeSets

        // First version from History contains no paths, add them
        if (changeSets) {
            DocumentUtil.traverse(docVersions.first().doc.data) {_, path ->
                if (path && path.last() != JsonLd.TYPE_KEY && path.last() != JsonLd.ID_KEY) {
                    changeSets.first().addedPaths.add(path.collect())
                }
                return DocumentUtil.NOP
            }
        }

        var lastAgent = null
        var lastDay = null
        var editNo = 0
        int versionNo = 0
        var saves = []
        var isFirstManual = true

        if (shortId == 'bvnpzmmn4qf7nzs') {
            println(changeSets.join("\n\n\n"))
        }

        for (var save : changeSets) {
            String timestamp = save.date
            String year = timestamp.split("-").first()

            if (Integer.parseInt(year) < 2021 || save.tool != [ "@id": "https://id.kb.se/generator/crud" ]) {
                editNo++
                versionNo = editNo
                continue
            }

            String day = timestamp.split("T").first()
            String agent = save.agent["@id"]

            if(agent != lastAgent || day != lastDay) {
                if(saves) {
                    processVersionSession(shortId, versionNo, isFirstManual, saves)
                    saves = []
                    isFirstManual = false
                }

                versionNo = editNo
            }

            saves.add(save)

            lastAgent = agent
            lastDay = day

            editNo++
        }

        if(saves) {
            processVersionSession(shortId, versionNo, isFirstManual, saves)
        }
    }
    catch (Exception e) {
        errors.println("Error in ${bib.doc.shortId}: ${e}")
        e.printStackTrace()
    }
}

void processVersionSession(shortId, versionNo, isFirstManual, List saves) {
    var added = saves.collectMany { filteredPaths(it.addedPaths) } as Set
    var removed = saves.collectMany { filteredPaths(it.removedPaths) } as Set
    var modified = added.intersect(removed)

    var agent = saves.first().agent["@id"]
    String timestamp = saves.last().date

    added -= modified
    removed -= modified

    var sigel = Unicode.stripPrefix(agent, "https://libris.kb.se/library/")
    String year = timestamp.split("-").first()

    var createdOrModified =
            versionNo == 0
                ? "CREATE"
                : (isFirstManual
                    ? "UPGRADE"
                    : "MODIFY")

    StringBuilder s = new StringBuilder()
    var append = { operation, path ->
        s.append([shortId, timestamp, year, createdOrModified, versionNo, saves.size(), sigel, agent, operation, path].join('\t')).append("\n")
    }

    added.each { append("ADD", it)}
    removed.each { append("REMOVE", it)}
    modified.each { append("MODIFY", it)}

    byPath.print(s.toString())

    var allPaths = added + removed + modified
    if (allPaths) {
        byVersion.println([shortId, timestamp, year, createdOrModified, versionNo, saves.size(), sigel, agent, allPaths.join(",")].join('\t'))
    }
}

static Set<String> filteredPaths(Collection<List<Object>> paths) {
    paths.findResults {
        if (it.size() < 3 || it.get(0) != "@graph" || it.get(1) != 1) {
            return null;
        }
        it.drop(2).takeWhile { it instanceof String }.join(".")
    } as Set
}
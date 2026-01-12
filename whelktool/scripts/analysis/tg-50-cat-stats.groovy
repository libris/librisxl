/**
 *
 *
 *
 * See https://kbse.atlassian.net/browse/TG-50
 *
 */


import groovy.transform.Memoized
import whelk.Document
import whelk.JsonLd
import whelk.history.History
import whelk.util.DocumentUtil
import whelk.util.Unicode

byPath = getReportWriter("changes-by-path.tsv")
byVersion = getReportWriter("changes-by-version.tsv")
errors = getReportWriter("errors.txt")

whelk = getWhelk()

//selectByIds(['bvnpzmmn4qf7nzs']) {
selectByCollection('bib') {
    process(it)
}

void process(bib) {
    try {
        var shortId = bib.doc.getShortId()
        List docVersions = whelk.storage.loadDocumentHistory(shortId)
        var lastDoc = docVersions.last().doc
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

        for (var save : changeSets) {
            String timestamp = save.date
            String year = timestamp.split("-").first()

            String agent = save.agent["@id"]

            if (Integer.parseInt(year) < 2021
                    || save.tool != [ "@id": "https://id.kb.se/generator/crud" ]
                    || !agent.startsWith("https://libris.kb.se/library/") // History bug?? tool not set
            ) {
                editNo++
                versionNo = editNo
                continue
            }

            String day = timestamp.split("T").first()

            if(agent != lastAgent || day != lastDay) {
                if(saves) {
                    processVersionSession(shortId, versionNo, isFirstManual, saves, lastDoc)
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
            processVersionSession(shortId, versionNo, isFirstManual, saves, lastDoc)
        }
    }
    catch (Exception e) {
        errors.println("Error in ${bib.doc.shortId}: ${e}")
        e.printStackTrace()
    }
}

void processVersionSession(shortId, versionNo, isFirstManual, List saves, Document lastDoc) {
    var added = saves.collectMany { filteredPaths(it.addedPaths) } as Set
    var removed = saves.collectMany { filteredPaths(it.removedPaths) } as Set
    var modified = added.intersect(removed)

    var agent = saves.first().agent["@id"]
    String timestamp = saves.last().date

    added -= modified
    removed -= modified

    var sigel = Unicode.stripPrefix(agent, "https://libris.kb.se/library/")
    String year = timestamp.split("-").first()

    var langs = getAtPath(lastDoc.data, ["@graph", 1, "instanceOf", "language", "*", "@id"], [])
            .collect { Unicode.stripPrefix(it, "https://id.kb.se/language/") }
            .sort()
            .join(",")

    var createdOrModified =
            versionNo == 0
                ? "CREATE"
                : (isFirstManual
                    ? "UPGRADE"
                    : "MODIFY")

    var libType = getLibType(agent)

    StringBuilder s = new StringBuilder()
    var append = { operation, path ->
        s.append([shortId, timestamp, year, createdOrModified, sigel, agent, libType, langs, operation, path].join('\t')).append("\n")
    }

    added.each { append("ADD", it)}
    removed.each { append("REMOVE", it)}
    modified.each { append("MODIFY", it)}

    byPath.print(s.toString())

    var allPaths = added + removed + modified
    if (allPaths) {
        byVersion.println([shortId, timestamp, year, createdOrModified, sigel, agent, libType, langs, allPaths.join(","), langs].join('\t'))
    }
}

@Memoized
getLibType(id) {
    var data = whelk.loadData(id)
    var types = getAtPath(data, ["@graph", 1, "bibdb:libraryType", "*", "@id"], []) + getAtPath(data, ["@graph", 1, "bibdb:libraryType", "@id"], [])

    var codes = types
            .collect { Unicode.stripPrefix(it, "https://id.kb.se/term/bibdb/") }
            .sort()
            .join(",")
}

static Set<String> filteredPaths(Collection<List<Object>> paths) {
    paths.findResults {
        if (it.size() < 3 || it.get(0) != "@graph" || it.get(1) != 1) {
            return null;
        }
        it.drop(2).takeWhile { it instanceof String }.join(".")
    } as Set
}
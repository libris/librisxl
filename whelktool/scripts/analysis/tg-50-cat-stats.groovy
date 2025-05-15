import whelk.history.History
import whelk.util.Unicode

byPath = getReportWriter("changes-by-path.txt")
byVersion = getReportWriter("changes-by-version.txt")
errors = getReportWriter("errors.txt")

whelk = getWhelk()

void process(bib) {
    try {
        var shortId = bib.doc.getShortId()
        History history = new History(whelk.storage.loadDocumentHistory(shortId), whelk.jsonld)
        List changeSets = history.m_changeSetsMap.changeSets

        var lastAgent = null
        var lastDay = null
        var editNo = 0
        int versionNo = 0
        var edits = []

        for (var version : changeSets) {
            if (version.tool != [ "@id": "https://id.kb.se/generator/crud" ]) {
                continue
            }

            String timestamp = version.date
            String year = timestamp.split("-").first()

            if (Integer.parseInt(year) < 2021) {
                continue
            }

            String day = timestamp.split("T").first()
            String agent = version.agent["@id"]

            if(agent != lastAgent || day != lastDay) {
                processEditSession(shortId, versionNo, edits)
                edits = []
                versionNo = editNo
            }

            edits.add(version)

            lastAgent = agent
            lastDay = day

            editNo++
        }
        processEditSession(shortId, versionNo, edits)
    }
    catch (Exception e) {
        errors.println("Error in ${bib.doc.shortId}: ${e}")
        e.printStackTrace()
    }
}

void processEditSession(shortId, versionNo, List edits) {
    if(!edits) {
        return
    }

    var added = edits.collectMany { filteredPaths(it.addedPaths) } as Set
    var removed = edits.collectMany { filteredPaths(it.removedPaths) } as Set
    var modified = added.intersect(removed)

    var agent = edits.first().agent["@id"]
    String timestamp = edits.last().date

    added -= modified
    removed -= modified

    var sigel = Unicode.stripPrefix(agent, "https://libris.kb.se/library/")
    String year = timestamp.split("-").first()

    StringBuilder s = new StringBuilder()
    var append = { operation, path ->
        s.append([shortId, timestamp, year, versionNo, edits.size(), sigel, agent, operation, path].join('\t')).append("\n")
    }

    added.each { append("ADD", it)}
    removed.each { append("REMOVE", it)}
    modified.each { append("MODIFY", it)}

    byPath.print(s.toString())

    var allPaths = added + removed + modified
    if (allPaths) {
        byVersion.println([shortId, timestamp, year, versionNo, edits.size(), sigel, agent, allPaths.join(",")].join('\t'))
    }
}

selectByCollection('bib') {
    process(it)
}


static Set<String> filteredPaths(Collection<List<Object>> paths) {
    paths.findResults {
        if (it.size() < 3 || it.get(0) != "@graph" || it.get(1) != 1) {
            return null;
        }
        it.drop(2).takeWhile { it instanceof String }.join(".")
    } as Set
}
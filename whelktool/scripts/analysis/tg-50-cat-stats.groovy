import whelk.Whelk
import whelk.history.History
import whelk.util.Unicode

PrintWriter byPath = getReportWriter("changes-by-path.txt")
PrintWriter byVersion = getReportWriter("changes-by-version.txt")
PrintWriter errors = getReportWriter("errors.txt")
class C {
    public static var CRUD = [ "@id": "https://id.kb.se/generator/crud" ]
}

Whelk whelk = getWhelk()

PrintWriter byPath = getReportWriter("changes-by-path.txt")
PrintWriter byVersion = getReportWriter("changes-by-version.txt")

process = { bib ->
    try {
        var shortId = bib.doc.getShortId()
        History history = new History(whelk.storage.loadDocumentHistory(shortId), whelk.jsonld)
        List changeSets = history.m_changeSetsMap.changeSets
        int versionNo = 0
        for (var version : changeSets) {
            if (version.tool != C.CRUD) {
                continue
            }

            String timestamp = version.date
            String year = timestamp.split("-").first()

            if (Integer.parseInt(year) < 2021) {
                continue
            }

            var added = filteredPaths(version.addedPaths)
            var removed = filteredPaths(version.removedPaths)
            var modified = added.intersect(removed)
            added -= modified
            removed -= modified
            String agent = version.agent["@id"]
            var sigel = Unicode.stripPrefix(agent, "https://libris.kb.se/library/")

            StringBuilder s = new StringBuilder()
            var append = { operation, path ->
                s.append([shortId, timestamp, year, versionNo, sigel, agent, operation, path].join('\t')).append("\n")
            }

            added.each { append("ADD", it)}
            removed.each { append("REMOVE", it)}
            modified.each { append("MODIFY", it)}

            byPath.print(s.toString())

            var allPaths = added + removed + modified
            if (allPaths) {
                byVersion.println([shortId, timestamp, year, versionNo, sigel, agent, allPaths.join(",")].join('\t'))
            }

            versionNo++
        }
    }
    catch (Exception e) {
        errors.println("Error in ${bib.doc.shortId}: ${e}")
        e.printStackTrace()
        return
    }
}

selectByCollection('bib') {
    process(it)
}

Set<String> filteredPaths(Collection<List<Object>> paths) {
    paths.findResults {
        if (it.size() < 3 || it.get(0) != "@graph" || it.get(1) != 1) {
            return null;
        }
        it.drop(2).takeWhile { it instanceof String }.join(".")
    } as Set
}
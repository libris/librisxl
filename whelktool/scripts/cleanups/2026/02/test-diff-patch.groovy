import whelk.Document
import whelk.diff.Diff
import whelk.diff.Patch

import static whelk.util.Jackson.mapper

PrintWriter failures = getReportWriter("FAILURES")

String where = "true"

selectBySqlWhere(where) { data ->
    List<Document> docVersions = data.whelk.storage.loadAllVersions(data.doc.shortId)
    List versions = docVersions.collect {it.data}

    def diffs = []
    for (int i = 0; i < versions.size()-1; ++i) {
        diffs.add( Diff.diff(versions[i], versions[i+1]) )
    }
    def recreatedVersions = [versions[0]]

    for (int i = 0; i < diffs.size(); ++i) {
        recreatedVersions.add( Patch.patch( versions[i], mapper.writeValueAsString(diffs[i])) )
    }

    if (!recreatedVersions.equals(versions)) {
        System.err.println("** CRITIAL! TEST FAILED! CHECK reports/FAILURES")
        failures.println("FAILED: " + versions + " produved diffs: " + diffs)
    }
}

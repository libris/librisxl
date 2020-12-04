import whelk.JsonLdValidator
import whelk.JsonLdValidator.Error as JsonError
import whelk.util.Statistics
import whelk.util.DocumentUtil

class Script {
    static PrintWriter report
    static Statistics s = new Statistics(6).printOnShutdown()
    static JsonLdValidator v
    static boolean isInitialized = false
    static Set<String> expectsSetsWithConflicts = new HashSet<>()
}

Script.report = getReportWriter("report.txt")

selectByCollection('bib') { rec ->
    synchronized (this) {
        if (!Script.isInitialized) {
            Script.v = JsonLdValidator.from(rec.whelk.jsonld)
            Script.v.skipUndefined()
            Script.isInitialized = true
        }
    }

    try {
        process(rec)
    }
    catch(Exception e) {
        System.err.println("${rec.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(rec) {
    Set<String> pathsToConflictingSetTerms = getPathsToConflictingSetTerms(rec)
    pathsToConflictingSetTerms.each {
        Script.s.increment("Term with conflicting repeatability", it, rec.doc.getShortId())
    }

    List<JsonError> errors = Script.v.validateAll(rec.doc.data)

    errors.each {
        String valsize = ''
        if (it.value instanceof List) {
            valsize = ":${representSize(it.value.size())}"
        }
        String message = "path: ${representPath(it.path)}${valsize}"
        if (it.type == JsonError.Type.UNKNOWN_VOCAB_VALUE ||
            it.type == JsonError.Type.UNEXPECTED) {
            message += " value: ${it.value}"
        }
        if (message in pathsToConflictingSetTerms) {
            message += ' (KNOWN)'
        }
        Script.s.increment(it.getDescription(), message, rec.doc.getShortId())
    }
    if (errors) {
        Script.report.println(rec.doc.shortId)
    }
}

String representPath(List path) {
    return path.join('.')
        .replaceAll(/^@graph\.0/, 'record')
        .replaceAll(/^@graph\.1/, 'instance')
        .replaceAll(/^@graph\.2/, 'work')
        .replaceAll(/\.\d+/, '[]')
}

String representSize(int size) {
    size > 4 ? '5+' : size > 1 ? '2+' : size.toString()
}

Set<String> getPathsToConflictingSetTerms(rec) {
    if (Script.expectsSetsWithConflicts.size() == 0) {
        synchronized (Script.expectsSetsWithConflicts) {
            rec.whelk.marcFrameConverter.conversion.badRepeats.each {
                Script.expectsSetsWithConflicts << it.term
            }
        }
    }
    Set<String> pathsToSetsWithList = new HashSet<>()
    DocumentUtil.traverse(rec.doc.data, { value, path ->
        if (!path) {
            return
        }
        String key = path.last()
        if (key in Script.expectsSetsWithConflicts) {
            String valsize = ''
            if (value instanceof List) {
                valsize = ":${representSize(value.size())}"
            }
            String message = "path: ${representPath(path)}${valsize}"
            pathsToSetsWithList << message
        }
        return
    })
    return pathsToSetsWithList
}

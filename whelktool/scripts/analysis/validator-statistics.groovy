import whelk.JsonLdValidator
import whelk.JsonLdValidator.Error as JsonError
import whelk.util.Statistics

class Script {
    static PrintWriter report
    static Statistics s = new Statistics(3).printOnShutdown()
    static JsonLdValidator v
    static boolean isInitialized = false
}

Script.report = getReportWriter("report.txt")

selectByCollection('bib') { bib ->
    synchronized (this) {
        if (!Script.isInitialized) {
            Script.v = JsonLdValidator.from(bib.whelk.jsonld)
            Script.isInitialized = true
        }
    }

    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println("${bib.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(bib) {
    List<JsonError> errors = Script.v.validateAll(bib.doc.data)
    errors.each {
        def message = "key: " + it.key
        if (it.type == JsonError.Type.UNKNOWN_VOCAB_VALUE ||
            it.type == JsonError.Type.UNEXPECTED) {
            message = message + " value: " + it.value
        }
        Script.s.increment(it.getDescription(), message, bib.doc.getShortId())
    }
    if (errors) {
        Script.report.println(bib.doc.shortId)
    }
}
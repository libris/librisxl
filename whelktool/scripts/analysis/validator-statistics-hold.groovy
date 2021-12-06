import whelk.JsonLdValidator
import whelk.JsonLdValidator.Error as JsonError
import whelk.util.Statistics

class Script {
    static PrintWriter report
    static Statistics s = new Statistics(6).printOnShutdown()
    static JsonLdValidator v
    static boolean isInitialized = false
}

Script.report = getReportWriter("report.txt")

selectByCollection('hold') { hold ->
    synchronized (this) {
        if (!Script.isInitialized) {
            Script.v = JsonLdValidator.from(hold.whelk.jsonld)
            Script.v.skipUndefined()
            Script.isInitialized = true
        }
    }

    try {
        process(hold)
    }
    catch(Exception e) {
        System.err.println("${hold.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(hold) {
    List<JsonError> errors = Script.v.validateAll(hold.doc.data)

    errors.each {
        def message = "key: " + it.key
        if (it.type == JsonError.Type.UNKNOWN_VOCAB_VALUE ||
            it.type == JsonError.Type.UNEXPECTED) {
            message = message + " value: " + it.value
        }
        Script.s.increment(it.getDescription(), message, hold.doc.getShortId())
    }
    if (errors) {
        Script.report.println(hold.doc.shortId)
    }
}

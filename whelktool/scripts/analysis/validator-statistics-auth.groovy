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

selectByCollection('auth') { auth ->
    synchronized (this) {
        if (!Script.isInitialized) {
            Script.v = JsonLdValidator.from(auth.whelk.jsonld)
            Script.v.skipUndefined()
            Script.isInitialized = true
        }
    }

    try {
        process(auth)
    }
    catch(Exception e) {
        System.err.println("${auth.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(auth) {
    List<JsonError> errors = Script.v.validateAll(auth.doc.data)

    errors.each {
        def message = "key: " + it.key
        if (it.type == JsonError.Type.UNKNOWN_VOCAB_VALUE ||
            it.type == JsonError.Type.UNEXPECTED) {
            message = message + " value: " + it.value
        }
        Script.s.increment(it.getDescription(), message, auth.doc.getShortId())
    }
    if (errors) {
        Script.report.println(auth.doc.shortId)
    }
}

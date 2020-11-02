import whelk.JsonValidator
import whelk.util.Statistics

class Script {
    static PrintWriter report
    static Statistics s = new Statistics().printOnShutdown()
    static JsonValidator v
    static boolean isInitialized = false
}

Script.report = getReportWriter("report.txt")

selectByCollection('bib') { bib ->
    if (!Script.isInitialized) {
        Script.v = JsonValidator.from(bib.doc.whelk.jsonld)
        Script.isInitialized = true
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
    def errors = Script.v.validateAll(bib.doc.data)
    errors.each {
        Script.s.increment('errors', it)
    }
    Script.report.println(bib.doc.shortId)
}
import whelk.JsonValidator
import whelk.util.Statistics

class Script {
    static PrintWriter report
    static Statistics s = new Statistics(5).printOnShutdown()
    static JsonValidator v
    static boolean isInitialized = false
}

Script.report = getReportWriter("report.txt")

selectByCollection('bib') { bib ->
    init(bib)
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println("${bib.doc.shortId} $e")
        e.printStackTrace()
    }
}

void init (bib) {
    if (!Script.isInitialized) {
        synchronized (this) {
            if (!Script.isInitialized) {
                Script.v = JsonValidator.from(bib.whelk.jsonld)
                Script.isInitialized = true
            }
        }
    }
}

void process(bib) {
    def errors = Script.v.validateAll(bib.doc.data)
    errors.each {
        Script.s.increment(it.split().take(2).join(' '), it, bib.doc.shortId)
    }
    if (errors) {
        Script.report.println(bib.doc.shortId)
    }
}
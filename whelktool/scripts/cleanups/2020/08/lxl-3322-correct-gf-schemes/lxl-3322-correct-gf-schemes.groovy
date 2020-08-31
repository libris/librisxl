/**
 *
 * See LXL-3322 for more information
 */

report = getReportWriter("report.txt")

class Script {
    static PrintWriter report
}

def file = new File(scriptDir, "gf_schemes.csv")

Map wrongToRight = [:]
file.eachLine { line ->
    def keyValue = line.split("\\t")
    wrongToRight[keyValue[0]] = keyValue[1]
}

Script.report = getReportWriter("report.txt")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println("${bib.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(bib) {

    def (record, thing) = bib.graph
    Map work = getWork(thing)

    if (!work || !work.genreForm) {
        return
    }

    def genreForm = work.genreForm
    genreForm.each { gf ->
        def right = wrongToRight[gf.code]
        if(right) {
            Script.report.println("${bib.doc.shortId} ${gf.code} ${right}")
            gf.code = right
            bib.scheduleSave()
        }
        
    }
}

Map getWork(def thing) {
    if(thing && thing['instanceOf'] && thing['instanceOf']['@type']) {
        return thing['instanceOf']
    }
    return null
}
/**
 *
 * See LXL-3322 for more information
 */

SCHEMES = ["https://id.kb.se/term/saogf",
           "https://id.kb.se/term/barngf",
           "https://id.kb.se/term/gmgpc/swe",
           "https://id.kb.se/term/saogf"]

SCHEME_PREFIX = "https://id.kb.se/term/"

file = new File(scriptDir, "gf_schemes.tsv")

WRONG_TO_RIGHT = [:]

file.eachLine { line ->
    def keyValue = line.split("\\t")
    WRONG_TO_RIGHT[keyValue[0]] = keyValue[1]
}

report = getReportWriter("report.txt")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch (Exception e) {
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

    def changed = false

    genreForm.replaceAll { gf ->
        if (!gf.inScheme) {
            return gf
        }

        def code = gf.inScheme.code
        def right = WRONG_TO_RIGHT[code]
        if (right && gf.inScheme.code != right) {
            report.println("Scheme changed: " + "${bib.doc.shortId} ${gf.inScheme.code} ${right}")
            gf.inScheme.code = right
        }

        def scheme = SCHEME_PREFIX + gf.inScheme.code
        def uri = scheme + "/" + strip(gf.prefLabel.trim())

        if (SCHEMES.contains(scheme) && exists(uri)) {
            report.println("Term linked: " + "${bib.doc.shortId} ${gf.prefLabel} ${['@id': uri]}")
            changed = true
            return ['@id': uri]
        } else if (exists(scheme)) {
            report.println("Scheme linked: " + "${bib.doc.shortId} ${gf.inScheme} ${['@id': scheme]}")
            changed = true
            gf.inScheme = ['@id': scheme]

            //Add extra field for codes like 'fast  (OCoLC)fst01726755'
            if (right.equals('fast') && code.contains('OCoLC')) {
                gf["marc:recordControlNumber"] = "("+ code.split('(')[1]
                report.println("Controlnumber added: " + "${bib.doc.shortId} ${code} ${gf["marc:recordControlNumber"]}")
            }
        }
        return gf
    }

    if (changed) {
        bib.scheduleSave()
    }
}

Map getWork(def thing) {
    if(thing && thing['instanceOf'] && thing['instanceOf']['@type']) {
        return thing['instanceOf']
    }
    return null
}

private static String strip(String s) {
    if (s.endsWith(".")) {
        return s.substring(0, s.length() - 1)
    } else {
        return s
    }
}

private boolean exists(uri) {
    boolean exists = false
    selectByIds([uri]) {
        if (it) {
            exists = true
        }
    }
    return exists
}
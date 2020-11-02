/**
 * Parse FAST subject id from bad inScheme.code
 * e.g subject.inScheme.code: "fast  (OCoLC)fst01740895"
 *   -> marc:recordControlNumber: (OCoLC)fst01740895
 *   -> sameAs: http://id.worldcat.org/fast/1740895
 *
 * See LXL-3416 for more info.
 */
modifiedReport = getReportWriter("modified.txt")
errorReport = getReportWriter("errors.txt")

File idFile = new File(scriptDir, 'ids.txt')
idFile.exists() ? selectByIds(idFile.readLines(), this.&handle) : selectByCollection('bib', this.&handle)

void handle(bib) {
    try {
        process(bib)
    }
    catch(Exception e) {
        errorReport.println("${bib.doc.shortId} $e")
        e.printStackTrace(errorReport)
        println("${bib.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(bib) {
    def instance = bib.graph[1]
    if (!instance.instanceOf) {
        return
    }

    asList(instance.instanceOf.subject).each { Map subject ->
        ((subject.inScheme?.code?:'') =~ /fast\s*(\(OCoLC\)fst0*(\d+))/ ).with {
            if (matches()) {
                subject['marc:recordControlNumber'] = group(1)
                subject.inScheme = ['@id': 'https://id.kb.se/term/fast']
                subject.sameAs = [['@id': "http://id.worldcat.org/fast/${group(2)}"]]
                modifiedReport.println("$bib.doc.shortId $subject")
                bib.scheduleSave()
            }
        }
    }
}

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}
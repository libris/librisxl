/**
 * See LXL-3304 for more information.
 */

class Script {
    static PrintWriter movedToInstance
    static PrintWriter movedToClassification
    static PrintWriter removed
    static PrintWriter equivalentCodes
    static PrintWriter errors
}

Script.movedToInstance = getReportWriter("moved-to-instance.txt")
Script.movedToClassification = getReportWriter("moved-to-classification.txt")
Script.removed = getReportWriter("removed.txt")
Script.equivalentCodes = getReportWriter("equivalent-codes.txt")
Script.errors = getReportWriter("errors.txt")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        Script.errors.println("${bib.doc.shortId} $e")
        e.printStackTrace(Script.errors)
    }
}

void process(bib) {
    def instance = bib.graph[1]
    def work = instance['instanceOf']

    if (!work) {
        return
    }

    def bib976 = asList(work['marc:hasBib976'])

    if (!bib976) {
        return
    }

    def (code, noCode) = bib976.split { it['marc:bib976-a'] }
    def bib084 = sab(work)

    handleWithSabCode(bib, work, bib084, code)
    handleWithoutSabCode(bib, work, instance, noCode)
}

void handleWithSabCode(bib, work, bib084, bib976) {
    def (in084, notIn084) = bib976.split {
        bib084.findAll { sab ->
            def codes = asList(sab)
            return codes.any { code ->
                haveEquivalentCodes(bib, code, it)
            }
        }
    }

    in084.each {
        bib976.remove(it)
        print(['bib976-a: ' + it['marc:bib976-a']],
                bib,
                Script.removed)
        bib.scheduleSave()
    }

    notIn084.each {
        work['classification'] = work['classification'] ?: []
        work['classification'].add(createClassification(it['marc:bib976-a'], it['marc:bib976-i2'] ?: 'n/a'))
        bib976.remove(it)
        print(['bib976-a: ' + it['marc:bib976-a']],
                bib,
                Script.movedToClassification)
        bib.scheduleSave()
    }
}

void handleWithoutSabCode(bib, work, instance, bib976) {
    bib976.each {
        copyToInstance(instance, it)
    }
    work.remove('marc:hasBib976')
    print(["bib976-a:" + bib976['marc:bib976-a']],
            bib,
            Script.movedToInstance)
    bib.scheduleSave()
}

void copyToInstance(instance, it) {
    instance['marc:hasBib976'] = instance['marc:hasBib976'] ?: []
    instance['marc:hasBib976'].add(it)
}

boolean haveEquivalentCodes(bib, code084, bib976) {
    def code976 = bib976['marc:bib976-a']
    if (code084?.startsWith(code976) ||
            (code084 == "He=c" && code976 == "Hcee") ||
            (code084 == "Hcee" && code976 == "He=c")) {

        print(["Classification code: " + code084, "bib976-a: " + code976],
                bib,
                Script.equivalentCodes)
        return true
    } else {
        return false
    }
}

Map createClassification(code, version) {
    return [
            "@type"   : "Classification",
            "code"    : code,
            "inScheme": [
                    ["@type": "ConceptScheme"],
                    ["code": "kssb"],
                    ["version": version]
            ]
    ]
}

List sab(work) {
    asList(work['classification'])
            .findAll{ it['inScheme'] ?: '' == 'kssb' }
            .collect{ it['code'] }
}

def asList(x) {
    (x ?: []).with {it instanceof List ? it : [it] }
}

void print(lines, bib, writer) {
    String full = "${bib.doc.getURI()}" + "\n"
    lines.each {full = full.concat(it + "\n")}
    writer.println(full.stripIndent())
}
removed = getReportWriter('removed.txt')
changed = getReportWriter('changed.txt')

def ids = new File(scriptDir, 'CREATED-20231212.txt').readLines()

selectByIds(ids) { docItem ->
    def id = docItem.doc.shortId
    def work = docItem.graph[1]

    def existsProperSab = hasProperSab(work)

    if (existsProperSab) {
        if (work['classification'].removeAll {
            isSab(it) && isBadCode(it.code)
        }) {
            docItem.scheduleSave()
            removed.println(id)
        }
    } else {
        work['classification']?.each {
            if (isSab(it) && isBadCode(it.code)) {
                if (it.code.startsWith('Hcb')) {
                    it['code'] = 'Hc' + (it.code =~ /\.\d+$/).with {it ? it[0] : '' }
                } else if (it.code.startsWith('Hdab')) {
                    it['code'] = 'Hda' + (it.code =~ /\.\d+$/).with {it ? it[0] : '' }
                }
                docItem.scheduleSave()
                changed.println(id)
            }
        }
    }
}

boolean isBadCode(String code) {
    code =~ /^Hcb|^Hdab/
}

boolean hasProperSab(Map work) {
    work['classification']?.any { isSab(it) && !(isBadCode(it.code) || it.code == 'H') }
}

boolean isSab(Map classification) {
    classification.inScheme?.code =~ /[Kk]ssb/
}

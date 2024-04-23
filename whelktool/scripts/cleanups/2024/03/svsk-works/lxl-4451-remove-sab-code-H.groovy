def ids = new File(scriptDir, 'CREATED-20231212.txt').readLines()

selectByIds(ids) { docItem ->
    def work = docItem.graph[1]

    if (hasSabFiction(work)) {
        if (work['classification'].removeAll {
            !isSab(it) && it.code == 'H'
        }) {
            docItem.scheduleSave()
        }
    }
}


boolean hasSabFiction(Map work) {
    work['classification']?.any { isSab(it) && it.code.startsWith('H') }
}

boolean isSab(Map classification) {
    classification.inScheme?.code =~ /[Kk]ssb/
}


/*
Add instanceOf.genreForm saogf/Dagstidningar to listed docs

 */
notModified = getReportWriter("not-modified.txt")

selectByIds(new File(scriptDir, 'Libris-id_saogf_Dagstidningar.txt').readLines()) { bib ->
    def (_record, thing) = bib.graph
    
    if (thing.issuanceType != 'Serial') {
        notModified.println("${bib.doc.shortId} Wrong issuanceType")
        return
    }

    if (thing.'@type' != 'Electronic') {
        notModified.println("${bib.doc.shortId} Not Electronic")
    }
    
    def genreForm = (thing.instanceOf.genreForm ?: []) as Set
    
    genreForm += ['@id' : 'https://id.kb.se/term/saogf/Dagstidningar']
    thing.instanceOf.genreForm = genreForm as List
    
    bib.scheduleSave()
}
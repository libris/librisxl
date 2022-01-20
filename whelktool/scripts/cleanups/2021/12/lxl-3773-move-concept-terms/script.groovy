PrintWriter movedToSubject = getReportWriter('moved-to-subject.tsv')
PrintWriter movedToGf = getReportWriter('moved-to-gf.tsv')
PrintWriter replacedByGf = getReportWriter('replaced-by-gf.tsv')

List saogfSchemes = ["https://id.kb.se/term/saogf", "https://id.kb.se/term/barngf"]
Set moveToGf = queryDocs(["inScheme.@id": saogfSchemes]).collect { it.'@id' } as Set
Set moveToSubject = [] as Set
Map replaceByGf = [:]

new File(scriptDir, 'topics-in-gf-counted.tsv').eachLine {line, number ->
    if (number == 1)
        return

    List cols = line.split('\t')

    String saoTerm = cols[0]
    String saoGfTerm = cols[4]
    String move = cols[5]

    if (saoGfTerm)
        replaceByGf[saoTerm] = saoGfTerm
    else if (move)
        moveToSubject << saoTerm
}

selectByCollection('bib') { data ->
    def work = data.graph[1].instanceOf
    String id = data.doc.shortId

    if (!work)
        return

    boolean modified = false

    if (work.genreForm) {
        // Move misplaced topics from genreForm to subject
        modified = work.genreForm.removeAll { concept ->
            String conceptId = concept.'@id'

            if (conceptId in moveToSubject) {
                work['subject'] = work.subject ?: []
                if (!work.subject.contains(concept))
                    work.subject << concept
                movedToSubject.println("$id\t$conceptId")
                return true
            }

            return false
        }

        // Replace topics (in genreForm) with corresponding gf terms
        work.genreForm.each { concept ->
            String conceptId = concept.'@id'

            if (replaceByGf.containsKey(conceptId)) {
                concept.'@id' = replaceByGf[conceptId]
                modified = true
                replacedByGf.println("$id\t$conceptId\t${replaceByGf[conceptId]}")
            }
        }
    }

    // Move misplaced gf terms from subject to genreForm
    if (work.subject) {
        modified |= work.subject.removeAll { concept ->
            String conceptId = concept.'@id'

            if (conceptId in moveToGf) {
                work['genreForm'] = work.genreForm ?: []
                if (!work.genreForm.contains(concept))
                    work.genreForm << concept
                movedToGf.println("$id\t$conceptId")
                return true
            }

            return false
        }
    }

    if (work.genreForm?.isEmpty())
        work.remove('genreForm')

    if (work.subject?.isEmpty())
        work.remove('subject')

    if (modified)
        data.scheduleSave()
}
PrintWriter gfToSubject = getReportWriter('gf-to-subject.tsv')
PrintWriter subjectToGf = getReportWriter('subject-to-gf.tsv')
//PrintWriter complexInGf = getReportWriter('complex-in-gf.tsv')
//PrintWriter blankInGf = getReportWriter('blank-in-gf.tsv')
//PrintWriter complexInSubject = getReportWriter('complex-in-subject.tsv')
//PrintWriter blankInSubject = getReportWriter('blank-in-subject.tsv')

List saoSchemes = ["https://id.kb.se/term/sao", "https://id.kb.se/term/barn"]
List saogfSchemes = ["https://id.kb.se/term/saogf", "https://id.kb.se/term/barngf"]

Map saoTerms = queryDocs(["inScheme.@id": saoSchemes]).collectEntries { [it.'@id', it.inScheme.'@id'] }
Map saogfTerms = queryDocs(["inScheme.@id": saogfSchemes]).collectEntries { [it.'@id', it.inScheme.'@id'] }

selectByCollection('bib') { data ->
    Map work = data.graph[1].instanceOf
    String id = data.doc.shortId

    if (!work)
        return

    boolean modified = work.genreForm?.removeAll { concept ->
        String conceptId = concept.'@id'
        if (saoTerms.containsKey(conceptId)) {
            work.subject = work.subject ?: []
            if (!work.subject.contains(concept))
                work.subject << concept

            gfToSubject.println("$id\t${saoTerms[conceptId]}\t$conceptId")

            return true
        }

//        String schemeId = concept.inScheme?.'@id'
//        if (schemeId in saoSchemes) {
//            if (concept.'@type' == "ComplexSubject") {
//                complexInGf.println("$id\t$schemeId\t${concept.prefLabel}")
//            } else {
//                blankInGf.println("$id\t$schemeId\t${concept.prefLabel}")
//            }
//        }

        return false
    }

    modified |= work.subject?.removeAll { concept ->
        String conceptId = concept.'@id'

        if (saogfTerms.containsKey(conceptId)) {
            work.genreForm = work.genreForm ?: []
            if (!work.genreForm.contains(concept))
                work.genreForm << concept

            subjectToGf.println("$id\t${saogfTerms[conceptId]}\t$conceptId")

            return true
        }

//        String schemeId = concept.inScheme?.'@id'
//        if (schemeId in saogfSchemes) {
//            if (concept.'@type' == "ComplexSubject") {
//                complexInSubject.println("$id\t$schemeId\t${concept.prefLabel}")
//            } else {
//                blankInSubject.println("$id\t$schemeId\t${concept.prefLabel}")
//            }
//        }

        return false
    }

    if (work.genreForm?.isEmpty())
        work.remove('genreForm')

    if (work.subject?.isEmpty())
        work.remove('subject')

    if (modified)
        data.scheduleSave()
}
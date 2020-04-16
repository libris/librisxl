/**
 *  Add contribution.role "author" IFF
 *  - Work @type is 'Text'
 *  - There isn't any "author" in contribution
 *  - There is one contribution without role
 *
 *  See LXL-3099 for more info
 */

final String AUTHOR = 'https://id.kb.se/relator/author'

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectByCollection('bib') { bib ->
    Map work = getWork(bib)

    if(!work || work['@type'] != 'Text' || !work['contribution']) {
        return
    }

    if (work['contribution']['role']['@id'].flatten().contains(AUTHOR)) {
        return
    }

    int numContributions = work['contribution'].size()
    int numWithRole = work['contribution']['role'].grep().size()

    if (numContributions - numWithRole == 1) {
        scheduledForUpdate.println("${bib.doc.getURI()} ${work['contribution']}")
        work['contribution'].each {
            if (!it['role']) {
                it['role'] = ['@id': AUTHOR]
            }
        }
        bib.scheduleSave()
    }

}


Map getWork(def bib) {
    def (record, thing, work) = bib.graph
    if (thing && isInstanceOf(thing, 'Work')) {
        return thing
    }
    else if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    }
    else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}
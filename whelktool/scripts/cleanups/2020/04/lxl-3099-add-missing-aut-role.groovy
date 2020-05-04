/**
 * Since 2016, practice is to add contribution.role for "author" in accordance with RDA.
 *
 *  Add contribution.role "author" IFF
 *  - There isn't any "author" in contribution
 *  - There is one primary contribution without role
 *  - Work @type is 'Text'
 *  - Instance type is one of 'Instance', 'Electronic'
 *  - issuanceType is 'Monograph'
 *  - no 'musicFormat', 'musicKey' or 'musicMedium' field
 *
 *  See LXL-3099 for more info
 */

final String AUTHOR = 'https://id.kb.se/relator/author'
final List NON_TEXT_FIELDS = ['musicFormat', 'musicKey', 'musicMedium']

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectByCollection('bib') { bib ->
    Map work = getWork(bib)
    Map instance = getInstance(bib)

    if(!work || !instance
            || work['@type'] != 'Text'
            || !work['contribution']
            || !(instance['@type'] in ['Instance', 'Electronic'])
            || !instance['issuanceType'] != 'Monograph'
            || NON_TEXT_FIELDS.any{ work.containsKey(it) || instance.containsKey(it) }
    ) {
        return
    }

    if (work['contribution']['role']['@id'].flatten().contains(AUTHOR)) {
        return
    }

    List noRole = work['contribution'].findAll{!it['role']}
    if (noRole.size() == 1 && noRole.first()['@type'] == "PrimaryContribution") {
        scheduledForUpdate.println("${bib.doc.getURI()} ${work['contribution']}")
        noRole.first()['role'] = ['@id': AUTHOR]
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

Map getInstance(def bib) {
    def (record, thing) = bib.graph
    if (thing && isInstanceOf(thing, 'Instance')) {
        return thing
    }
    return null
}
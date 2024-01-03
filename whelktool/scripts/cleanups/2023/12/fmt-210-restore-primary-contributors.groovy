/**
 * Follow-up on LXL-2512 that moved instance bound contributions from work to instance.
 * As a side effect of that move some agents expected to remain in the work's PrimaryContribution instead ended up in instance.contribution.
 *
 * See FMT-210 for details.
 */

unhandled = getReportWriter('unhandled.tsv')
addedPrimary = getReportWriter('added-primary.tsv')

def roles = [
        'https://id.kb.se/relator/engraver',
        'https://id.kb.se/relator/lithographer',
        'https://id.kb.se/relator/etcher',
        'https://id.kb.se/relator/woodcutter',
        'https://id.kb.se/relator/woodEngraver',
        'https://id.kb.se/relator/electrotyper'
]
        .collect { ['@id': it] } as Set

def scriptId = "https://libris.kb.se/sys/globalchanges/2023/05/lxl-2512-move-contribution-by-relator-domain/script.groovy"

selectBySqlWhere("collection = 'bib' and deleted = false and data#>>'{@graph,1,contribution}' is not null") { bib ->
    def id = bib.doc.shortId
    def instance = bib.graph[1]
    def work = instance.instanceOf

    if (!work || work['@id']) return

    def oldestToNewestVersion
    try {
        oldestToNewestVersion = bib.getVersions()
    } catch (Exception e) {
        println(e.getStackTrace())
        return
    }
    def versionTouchedByScriptIdx = oldestToNewestVersion.findIndexOf { it.data['@graph'][0]['generationProcess']?['@id'] == scriptId }

    if (versionTouchedByScriptIdx == -1) return

    def versionBefore = oldestToNewestVersion[versionTouchedByScriptIdx - 1]
    def workContributionBefore = versionBefore.data['@graph'][1]['instanceOf']['contribution']
    def primaryBefore = workContributionBefore.find { it['@type'] == 'PrimaryContribution' }

    if (asList(primaryBefore?['role']).intersect(roles).isEmpty()) return

    // Reaching here means that one of the concerned roles was moved from PrimaryContribution on work to "just" Contribution on instance by lxl-2512.

    def currentPrimary = work['contribution']?.find { it['@type'] == 'PrimaryContribution' && it['agent'] == primaryBefore['agent'] }

    // Decide which role should be in PrimaryContribution based on work type.
    def newPrimaryRole = work['@type'] == 'StillImage'
            ? ['@id': 'https://id.kb.se/relator/artist']
            : (work['@type'] == 'Cartography' ? ['@id': 'https://id.kb.se/relator/cartographer'] : null)

    def roleShort = { it['@id'].replaceAll(".*/", "") }

    if (!newPrimaryRole) {
        // Unable to decide which role to put in PrimaryContribution
        unhandled.println([id, work['@type'], asList(currentPrimary?['role']).collect(roleShort)].join('\t'))
        return
    }

    if (currentPrimary) {
        // The agent is still present in work PrimaryContribution
        if (!asList(currentPrimary['role']).contains(newPrimaryRole)) {
            // The current role(s) is neither artist nor cartographer, report.
            unhandled.println([id, work['@type'], asList(currentPrimary['role']).collect(roleShort)].join('\t'))
        }
    } else {
        // The agent is no longer present in work PrimaryContribution, readd it with relevant role.
        def newPrimary = ['@type': 'PrimaryContribution', 'agent': primaryBefore['agent'], 'role': [newPrimaryRole]]
        work['contribution'] = [newPrimary] + asList(work['contribution'])
        addedPrimary.println([id, newPrimary['agent'], newPrimary['role'].collect(roleShort)].join('\t'))
        bib.scheduleSave()
    }
}

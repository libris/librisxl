/**
 * Merge contributions having the same agent. Prefer merging into PrimaryContribution.
 * Example:
 * [
 *  {
 *      "@type": "PrimaryContribution",
 *      "agent": {"@id": "https://libris.kb.se/X#it"},
 *      "role": [{"@id": "https://id.kb.se/relator/author"}]
 *  },
 *  {
 *      "@type": "Contribution",
 *      "agent": {"@id": "https://libris.kb.se/X#it"},
 *      "role": [{"@id": "https://id.kb.se/relator/illustrator"}]
 *  }
 * ]
 * --->
 * [
 *  {
 *      "@type": "PrimaryContribution",
 *      "agent": {"@id": "https://libris.kb.se/X#it"},
 *      "role": [{"@id": "https://id.kb.se/relator/author"}, {"@id": "https://id.kb.se/relator/illustrator"}]
 *  }
 * ]
 */

import static se.kb.libris.mergeworks.Util.AGENT
import static se.kb.libris.mergeworks.Util.CONTRIBUTION
import static se.kb.libris.mergeworks.Util.PRIMARY
import static se.kb.libris.mergeworks.Util.ROLE
import static whelk.JsonLd.TYPE_KEY

def ids = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }.flatten()

selectByIds(ids) { bib ->
    def work = bib.graph[1].instanceOf
    def contribution = work?[CONTRIBUTION]

    if (!contribution) return

    def duplicates = contribution.countBy { asList(it[AGENT]) }.findResults { it.value > 1 ? it.key : null }

    duplicates.each { d ->
        def primaryContributionIdx = contribution.findIndexOf { asList(it[AGENT]) == d && it[TYPE_KEY] == PRIMARY }
        def mergeIntoIdx = primaryContributionIdx > -1
                ? primaryContributionIdx
                : contribution.findIndexOf { asList(it[AGENT]) == d }
        def mergeInto = contribution[mergeIntoIdx]

        def roles = contribution.findResults { asList(it[AGENT]) == d ? asList(it[ROLE]) : null }.flatten().unique()
        if (roles) mergeInto[ROLE] = roles

        def idx = 0
        contribution.removeAll {
            def removeIf = asList(it[AGENT]) == d && idx != mergeIntoIdx
            idx += 1
            return removeIf
        }
    }

    if (duplicates) {
        bib.scheduleSave()
    }
}
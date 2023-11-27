// LXL-2512: Move contribution by relator domain

import whelk.converter.marc.ContributionByRoleStep

unhandled = getReportWriter('unhandled.txt')

ruleSets = getWhelk().marcFrameConverter.conversion.marcRuleSets
contribStep = ruleSets.bib.postProcSteps.find { it instanceof ContributionByRoleStep }

// TODO: Optimize to only select those with instance relations?
def where = """
  collection = 'bib' AND deleted = false
  AND data#>'{@graph, 1, instanceOf, contribution}' notnull
"""

selectBySqlWhere(where) {
    def instance = it.graph[1]

    try {
        if (contribStep.moveRoles(instance)) {
            it.scheduleSave()
        }
    } catch (Exception e) {
        unhandled.println("${it.doc.shortId}: ${e}")
    }
}

// LXL-2512: Move contribution by relator domain
import whelk.converter.marc.ContributionByRoleStep

ruleSets = getWhelk().marcFrameConverter.conversion.marcRuleSets
contribStep = ruleSets.bib.postProcSteps.find { it instanceof ContributionByRoleStep }

// TODO: Optimize to only select those with instance relations?
def where = """
  collection = 'bib' AND deleted = false
  AND data#>'{@graph, 1, instanceOf, contribution}' notnull
"""

selectBySqlWhere(where) {
    def instance = it.graph[1]
    def instanceType = instance[TYPE]
    if (contribStep.moveRoles(instance)) {
        it.scheduleSave()
    }
}

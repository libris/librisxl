import whelk.converter.marc.NormalizeContentTypeStep

ruleSets = getWhelk().marcFrameConverter.conversion.marcRuleSets
contentTypeStep = ruleSets.bib.postProcSteps.find { it instanceof NormalizeContentTypeStep }

def where = """
  collection = 'bib' AND deleted = false
  AND data#>'{@graph, 1, instanceOf, hasPart}' IS NOT NULL
"""

selectBySqlWhere(where) {
    def instance = it.graph[1]
    if (contentTypeStep.moveContentType(instance)) {
        it.scheduleSave()
    }
}

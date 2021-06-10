String where = """
collection = 'bib' AND
(
  data#>>'{@graph,1,identifiedBy}' LIKE '%marc:OtherIdentifier%' OR
  data#>>'{@graph,1,indirectlyIdentifiedBy}' LIKE '%marc:OtherIdentifier%' OR
  data#>>'{@graph,1,instanceOf,identifiedBy}' LIKE '%marc:OtherIdentifier%'
) OR
collection = 'hold' AND
(
  data#>>'{@graph,1,identifiedBy}' LIKE '%marc:OtherIdentifier%'
)
"""

selectBySqlWhere(where) { data ->
    boolean modified = false

    Map mainEntity = data.graph[1]

    modified |= changetoIdentifier(mainEntity.identifiedBy)
    modified |= changetoIdentifier(mainEntity.indirectlyIdentifiedBy)
    modified |= changetoIdentifier(mainEntity.instanceOf.indirectlyIdentifiedBy)

    if (modified) {
        data.scheduleSave()
    }
}

boolean changetoIdentifier(List list) {
    boolean modified = false

    if (list != null &&
            list instanceof List &&
            !list.isEmpty()) {
        list.each {
            if (it["@type"] == "marc:OtherIdentifier") {
                it["@type"] = "Identifier"
                modified = true
            }
        }
    }

    return modified
}

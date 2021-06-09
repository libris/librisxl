String where = """
(
    (
        data#>>'{@graph,1}' LIKE '%marc:otherPhysicalDetails%'
        OR
        data#>'{@graph,0,marc:modifiedRecord}' is not null
    ) AND collection = 'bib'
)
"""

selectBySqlWhere(where) { data ->
    def (record, instance) = data.graph
    boolean changed = false

    changed |= removePhysical(instance)
    if (instance["hasPart"]) {
        instance["hasPart"].each { changed |= removePhysical(it) }
    }

    if (record["marc:modifiedRecord"]) {
        record.remove("marc:modifiedRecord")
        changed = true
    }

    if (changed)
        data.scheduleSave()
}

boolean removePhysical(Map entity) {
    if (entity["marc:otherPhysicalDetails"]) {
        entity["physicalDetailsNote"] = entity["marc:otherPhysicalDetails"]
        entity.remove("marc:otherPhysicalDetails")
        return true
    }
    return false
}

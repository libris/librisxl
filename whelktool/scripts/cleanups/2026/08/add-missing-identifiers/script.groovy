List identifiers = new File(scriptDir, "identifiers.csv").readLines()

identifiers.drop(1).each {row ->
    List cols = row.split(",")
    String librisSystemId = cols[0]
    String identifierType = cols[1]
    String gridOrEanIdentifer = cols[2]

    selectByIds([librisSystemId]) {  d ->
        def (record, thing) = d.graph
        def idBy = asList(thing.identifiedBy)
        idBy.addAll(['@type' : identifierType, 'value' : gridOrEanIdentifer])
        thing['identifiedBy'] = idBy
        d.scheduleSave()
    }
}

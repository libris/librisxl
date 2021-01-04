import whelk.util.DocumentUtil

String where = """
    collection = 'bib' 
    AND (data#>>'{@graph,1,associatedMedia}' LIKE '%http://www.fastighetswiki.se/wiki%' 
        OR data#>>'{@graph,1,marc:versionOfResource}' LIKE '%http://www.fastighetswiki.se/wiki%'
        OR data#>>'{@graph,1,isPrimaryTopicOf}' LIKE '%http://www.fastighetswiki.se/wiki%'
        OR data#>>'{@graph,1,relatedTo}' LIKE '%http://www.fastighetswiki.se/wiki%')
    """

List keys856 = ['associatedMedia', 'marc:versionOfResource', 'isPrimaryTopicOf', 'relatedTo']

List controlNumbers = Collections.synchronizedList([])
List ids = Collections.synchronizedList([])

// First get id and controlNumber of the fastighetswiki records
selectBySqlWhere(where) { data ->
    controlNumbers << data.doc.getControlNumber()
    ids << data.doc.getShortId()
}

selectByIds(ids) { data ->
    Map instance = data.graph[1]
    Map _856 = instance.findAll { it.getKey() in keys856 }

    DocumentUtil.traverse(_856) { value, path ->
        if (value instanceof String && value.startsWith('http')) {
            //All URLs start with 'http://www.fastighetswiki.se/wiki', thus there are no other working links via 856
            assert value.startsWith('http://www.fastighetswiki.se/wiki')

            if (instance['@type'] == 'Electronic') {
                data.scheduleDelete()
            } else {
                // Remove key containing the broken link
                instance.remove(path.find { it in keys856 })

                // Remove otherPhysicalFormat
                if (instance.containsKey('otherPhysicalFormat')) {
                    List otherPhysicalFormat = instance.otherPhysicalFormat
                    // otherPhysicalFormat refers to only one thing
                    assert otherPhysicalFormat.size() == 1

                    if (otherPhysicalFormat[0].containsKey('describedBy')) {
                        List describedBy = otherPhysicalFormat[0].describedBy
                        assert describedBy.size() == 1 && describedBy[0].containsKey('controlNumber')
                        String controlNumber = describedBy[0].controlNumber
                        // otherPhysicalFormat refers to a fastighetswiki record
                        assert controlNumber in controlNumbers
                        // so we can remove otherPhysicalFormat
                        instance.remove('otherPhysicalFormat')
                    } else {
                        // In one case (p712jz615vcjf7b), the describedBy property is missing in otherPhysicalFormat:
                        // println(data.doc.getShortId() + " has no describedBy")
                        // However it refers to a fastighetswiki record (looked up manually), thus otherPhysicalFormat can be removed
                        instance.remove('otherPhysicalFormat')
                    }
                }
                data.scheduleSave()
            }
        }
    }
}

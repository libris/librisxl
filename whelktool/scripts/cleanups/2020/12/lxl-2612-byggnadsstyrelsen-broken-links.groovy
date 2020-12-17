import whelk.util.DocumentUtil

String where = """
    collection = 'bib' 
    AND (data#>>'{@graph,1,associatedMedia}' LIKE '%http://www.fastighetswiki.se/wiki%' 
        OR data#>>'{@graph,1,marc:versionOfResource}' LIKE '%http://www.fastighetswiki.se/wiki%'
        OR data#>>'{@graph,1,isPrimaryTopicOf}' LIKE '%http://www.fastighetswiki.se/wiki%'
        OR data#>>'{@graph,1,relatedTo}' LIKE '%http://www.fastighetswiki.se/wiki%')
    """

List keys856 = ['associatedMedia', 'marc:versionOfResource', 'isPrimaryTopicOf', 'relatedTo']

selectBySqlWhere(where) { data ->
    Map instance = data.graph[1]
    Map _856 = instance.findAll { it.getKey() in keys856 }

    DocumentUtil.traverse(_856) { value, path ->
        if (value instanceof String && value.startsWith('http')) {
            //All URLs start with 'http://www.fastighetswiki.se/wiki', thus there are no other working links via 856
            assert value.startsWith('http://www.fastighetswiki.se/wiki')

            if (instance['@type'] == 'Electronic') {
                data.scheduleDelete()
            } else {
                instance.remove(path.find { it in keys856 })
                instance.remove('otherPhysicalFormat')
                data.scheduleSave()
            }
        }
    }
}
selectBySqlWhere("""
    collection = 'auth'
    and deleted = false
    and data#>>'{@graph,1,@type}' = 'Text'
    and data#>'{@graph,0,hasChangeNote}' @> '[{"tool":{"@id":"https://id.kb.se/generator/mergeworks"}}]' 
""") { auth ->
    auth.graph[1]['classification']?.each { c ->
        if (c['inScheme']?['version'] == "-1") {
            c['inScheme'].remove('version')
            auth.scheduleSave()
        }
    }
}

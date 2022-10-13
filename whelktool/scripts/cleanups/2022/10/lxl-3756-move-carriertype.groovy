String where = """
data['@graph'][1]['hasPart'][0]['carrierType'] IS NOT NULL
"""

OBJECT_TO_MOVE = ["@id": "https://id.kb.se/marc/OnlineResource"]

selectBySqlWhere(where) { data ->
    sourceList = data.graph[1].hasPart[0].carrierType
    targetList = data.graph[1].carrierType
    if(sourceList.contains(OBJECT_TO_MOVE)) {
        if(!targetList.contains(OBJECT_TO_MOVE)) {
            targetList.add(OBJECT_TO_MOVE)
        }
        data.graph[1].remove('hasPart') // see comment in Jira
        data.scheduleSave()
    }
}
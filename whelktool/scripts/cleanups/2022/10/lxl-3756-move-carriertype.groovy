String where = """
data['@graph'][1]['hasPart'][0]['carrierType'] IS NOT NULL
"""

OBJECT_TO_MOVE = ["@id": "https://id.kb.se/marc/OnlineResource"]
TYPE = "@type"
ELECTRONIC = "Electronic"

selectBySqlWhere(where) { data ->
    def sourceList = data.graph[1].hasPart[0].carrierType

    if(sourceList.contains(OBJECT_TO_MOVE)) {
        data.graph[1].carrierType = data.graph[1].carrierType ?: []
        targetList = data.graph[1].carrierType
        if(!targetList.contains(OBJECT_TO_MOVE)) {
            targetList.add(OBJECT_TO_MOVE)
        }
        sourceList.removeElement(OBJECT_TO_MOVE)
        if(sourceList.isEmpty()) {
            sourceList = data.graph[1].hasPart[0].remove('carrierType')
        }
        if(data.graph[1].hasPart[0].size() == 1 && data.graph[1].hasPart[0][TYPE] == ELECTRONIC) {
            data.graph[1].hasPart.remove(0)
            if(data.graph[1].hasPart.isEmpty()) {
                data.graph[1].remove('hasPart')
            }
        }
        data.graph[1][TYPE] = ELECTRONIC
        data.scheduleSave()
    }
}
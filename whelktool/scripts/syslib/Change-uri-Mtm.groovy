String where = """
    collection = 'hold'
    AND deleted = 'false'
    AND data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Mtm'
    AND data#>'{@graph,1,associatedMedia}' IS NOT NULL
  """


//selectByIds(['kvqshnmmh6pxpl71']){ hold ->

selectBySqlWhere(where) { hold ->

    def item = hold.graph[1]
    def itemOf = item["itemOf"]["@id"].replace("https://libris.kb.se/","").replace("#it","")

    String newUri = "https://www.legimus.se/bok/?librisId=$itemOf"

        if (item.associatedMedia[0].uri) { 
            if (item.associatedMedia[0].uri instanceof List) {
                item.associatedMedia[0].uri.clear()
                item.associatedMedia[0].uri.add(newUri)
            } else {
                item.associatedMedia[0].uri = newUri
            }

            hold.scheduleSave(loud: true)
        }
}
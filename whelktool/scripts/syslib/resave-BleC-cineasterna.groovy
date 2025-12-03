String qry = """
    collection = 'hold'
    AND deleted = 'false'
    AND data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/BleC'
    AND data#>'{@graph,1,associatedMedia}' IS NOT NULL
"""

selectBySqlWhere(qry) { hold ->

	def item = hold.graph[1]

	item.associatedMedia.each { am ->
			am.uri.each { uri -> 
				if ( uri.contains('www.cineasterna.com') ) {
					//resave
					//println(uri)
					hold.scheduleSave(loud: true)
				}
			}
	}

}

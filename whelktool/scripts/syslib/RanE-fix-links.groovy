//231220 KP, fixes links for RanE holds

String RanE_holds = """
        collection = 'hold' 
        and data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/RanE' 
        and deleted = false
        """

selectBySqlWhere(RanE_holds) { d ->
	def hold = d.getGraph()
	def instance = hold[1]
	if ( instance.containsKey('isPrimaryTopicOf') ) {
		if ( instance['isPrimaryTopicOf'] instanceof List ) {
			instance['isPrimaryTopicOf'].each { i ->
				if ( i.containsKey('uri') ) {
					if ( i['uri'] instanceof List ) {
						i['uri'].eachWithIndex { ii, iii ->
							if ( ii instanceof String && ii.startsWith("http://\$") ) {
								i['uri'].set(iii, "https://bibliotek.ranrike.se/digitala-biblioteket")
								//println(hold)
								//println("Modified ${instance.'@id'}")
								d.scheduleSave(loud: true)
							}
						}
					} else if ( i['uri'] instanceof String ) {
						if ( i['uri'].startsWith("http://\$") ) {
							i['uri'] = "https://bibliotek.ranrike.se/digitala-biblioteket"
							//println(hold)
							//println("Modified ${instance.'@id'}")
							d.scheduleSave(loud: true)
						}
					}
				}
			}
		} else if ( instance['isPrimaryTopicOf'] instanceof Map ) {
				if ( instance['isPrimaryTopicOf'].containsKey('uri') ) {
					if ( instance['isPrimaryTopicOf']['uri'] instanceof List ) {
						instance['isPrimaryTopicOf']['uri'].eachWithIndex { ii, iii ->
							if ( ii instanceof String && ii.startsWith("http://\$") ) {
						
								instance['isPrimaryTopicOf']['uri'].set(iii, "https://bibliotek.ranrike.se/digitala-biblioteket")
								//println(hold)
								//println("Modified ${instance.'@id'}")
								d.scheduleSave(loud: true)
							}
						}
					} else if ( instance['isPrimaryTopicOf']['uri'] instanceof String ) {
						if ( instance['isPrimaryTopicOf']['uri'].startsWith("http://\$") ) {
							instance['isPrimaryTopicOf']['uri'] = "https://bibliotek.ranrike.se/digitala-biblioteket"
							//println(hold)
							//println("Modified ${instance.'@id'}")
							d.scheduleSave(loud: true)
						}
					}
				}
		}
	}
}

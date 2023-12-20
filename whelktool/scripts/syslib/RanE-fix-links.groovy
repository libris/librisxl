//231220 KP, fixes links for RanE holds

String RanE_holds = """
        collection = 'hold' 
        and data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/RanE' 
        and deleted = false
        """

selectBySqlWhere(RanE_holds) { d ->
	def hold = d.getGraph()
	def instance = hold[1]
	//println("${instance.'@id'}")
	instance['isPrimaryTopicOf'].each { i ->
		i['uri'].eachWithIndex { ii, iii ->
			if ( ii.startsWith("http://\$") ) {
				i['uri'].set(iii, "https://bibliotek.ranrike.se/digitala-biblioteket")
				//println(hold)
				println("Modified ${instance.'@id'}")
				d.scheduleSave(loud: true)
			}
		}
	}
}

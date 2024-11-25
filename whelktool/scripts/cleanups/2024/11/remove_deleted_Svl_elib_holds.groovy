//241125 KP, removes all fake deleted holds for Svl with 'origin:Elib'

String Svl_elib_holds = """
        collection = 'hold' 
        and data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Svl' 
        and data#>'{@graph,1,cataloguersNote}' @> '["origin:Elib"]'::jsonb 
        and data#>'{@graph,1,cataloguersNote}' @> '["deleted"]'::jsonb
        and deleted = false
        """


selectBySqlWhere(Svl_elib_holds) { d ->
	//def hold = d.getGraph()
	//def id = hold[0].'@id'
	//println('I: ' + id)
	//println(hold)
	d.scheduleDelete(loud: true)
}

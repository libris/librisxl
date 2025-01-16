//241127 KP, removes all fake deleted holds for 'sigel' with 'origin:Elib'
// add -Dsigel=SIGEL to command line

def sigel = System.getProperty("sigel")

if ( sigel == null ) {
	println('set sigel with -Dsigel=SIGEL')
	System.exit(0)
}

def sigel_elib_holds = """
        collection = 'hold' 
        and data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/${sigel}' 
        and data#>'{@graph,1,cataloguersNote}' @> '["origin:Elib"]'::jsonb 
        and data#>'{@graph,1,cataloguersNote}' @> '["deleted"]'::jsonb
        and deleted = false
        """

selectBySqlWhere(sigel_elib_holds) { d ->
	//def hold = d.getGraph()
	//def id = hold[0].'@id'
	//println('I: ' + id)
	//println(hold)
	d.scheduleDelete(loud: true)
}

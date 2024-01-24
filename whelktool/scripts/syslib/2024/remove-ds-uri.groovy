File bibIDs = new File(scriptDir, "ds-id.txt")

selectByIds( bibIDs.readLines() ) { bib ->

	def instance = bib.graph[1]
        
        if (instance.electronicLocator) {
           		List eL = instance.electronicLocator
            	eL.removeAll { a ->
                	a.uri instanceof String && a.uri.contains('ds.kb.se') ||
               		a.uri instanceof List && a.uri.any { it.contains('ds.kb.se') }
            	}

            	if (eL.isEmpty()) {
                	instance.remove('electronicLocator')
            	}
        }

        bib.scheduleSave(loud: true)
    }

File bibIDs = new File(scriptDir, "affishx.txt")

selectByIds( bibIDs.readLines() ) { bib ->

	def instance = bib.graph[1]
        
        if (instance.electronicLocator) {
           		List eL = instance.electronicLocator
            	eL.removeAll { a ->
                	a.uri instanceof String && a.uri.contains('affischx.kb.se') ||
               		a.uri instanceof List && a.uri.any { it.contains('affischx.kb.se') }
            	}

            	if (eL.isEmpty()) {
                	instance.remove('electronicLocator')
            	}
        }

        bib.scheduleSave(loud: true)
    }

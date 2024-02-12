
File bibIDs = new File(scriptDir, "fulltext-kb.txt")

selectByIds( bibIDs.readLines() ) { bib ->

	def instance = bib.graph[1]
        
        if (instance.associatedMedia) {
           		List aM = instance.associatedMedia
            	aM.removeAll { a ->
                	a.uri instanceof String && a.uri.contains('fulltext.kb.se') ||
               		a.uri instanceof List && a.uri.any { it.contains('fulltext.kb.se') }
            	}

            	if (aM.isEmpty()) {
                	instance.remove('associatedMedia')
            	}
        }
        if (instance.isPrimaryTopicOf) {
                List iPTO = instance.isPrimaryTopicOf
                iPTO.removeAll { a ->
                    a.uri instanceof String && a.uri.contains('fulltext.kb.se') ||
                    a.uri instanceof List && a.uri.any { it.contains('fulltext.kb.se') }
                }

                if (iPTO.isEmpty()) {
                    instance.remove('isPrimaryTopicOf')
                }
        }

        bib.scheduleSave(loud: true)
    }
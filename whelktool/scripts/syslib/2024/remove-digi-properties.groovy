File bibIDs = new File(scriptDir, "72_poster_skolplanscher.txt")

selectByIds( bibIDs.readLines() ) { bib ->
def instance = bib.graph[1]

List propertiesToCheck = ["associatedMedia", "isPrimaryTopicOf", "usageAndAccessPolicy"]
boolean shouldChange = false

propertiesToCheck.each { prop ->
        if (instance[prop]) {
        	shouldChange = true
        }
    }

    if (shouldChange) {
    	instance.remove('associatedMedia')
        instance.remove('isPrimaryTopicOf')
        instance.remove('usageAndAccessPolicy')
    }
	
	bib.scheduleSave(loud: true)
}

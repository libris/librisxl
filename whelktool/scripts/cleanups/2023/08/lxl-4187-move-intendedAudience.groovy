String where = "collection = 'bib' and data#>>'{@graph,1,intendedAudience}' is not null"

selectBySqlWhere(where) { instance ->
    String type = instance.graph[1]["@type"]
    if (! instance.whelk.jsonld.isSubClassOf(type, "Instance") )
        return

    // instance-intendedAudiences are pre-verified to all be lists
    boolean instanceChanged = instance.graph[1].intendedAudience.removeAll { intendedAudience ->
        Map work = instance.graph[1].instanceOf
        if (work == null) // So much wierd data
            return false
        if (work.size() == 1 && work["@id"] != null) { // A linked work
            boolean workChanged = false
            selectByIds([work["@id"]]) { linkedWork ->
                workChanged = addIntendedAudienceToWork(intendedAudience, linkedWork.graph[1])
                if (workChanged) {
                    //System.err.println("Saving linked work with intendedAudience: " + linkedWork.graph[1].intendedAudience)
                    // Note to self: Is this safe (potentially saving the same record several times in quick succession)?
                    linkedWork.scheduleSave()
                }
            }
            return workChanged
        } else { // An embedded work
            boolean workChanged = addIntendedAudienceToWork(intendedAudience, instance.graph[1].instanceOf)
            //if (workChanged)
            //    System.err.println("Saving instance with embedded work with intendedAudience: " + instance.graph[1].instanceOf.intendedAudience)
            return workChanged
        }
    }

    if (instanceChanged) {
        instance.graph[1].remove("intendedAudience")
        instance.scheduleSave()
    }
}

private boolean addIntendedAudienceToWork(Map intendedAudience, Map work){
    if (work["intendedAudience"] == null)
        work["intendedAudience"] = []
    if (!work["intendedAudience"] instanceof List)
        work["intendedAudience"] = [work["intendedAudience"]]
    List intendedAudienceList = work["intendedAudience"]

    if (intendedAudienceList.contains(intendedAudience))
        return false

    intendedAudienceList.add(intendedAudience)
    return true
}

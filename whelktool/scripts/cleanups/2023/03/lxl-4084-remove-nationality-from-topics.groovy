/*
 * This removes nationality from all topics
 *
 * See LXL-4084 for more info.
 *
 */

selectBySqlWhere("""
        collection = 'auth' AND 
        (
            data#>>'{@graph,1,@type}' = 'Topic' OR 
            data#>>'{@graph,1,@type}' = 'Temporal' OR
            data#>>'{@graph,1,@type}' = 'ComplexSubject'
        ) AND
        data#>>'{@graph,1,nationality}' IS NOT NULL
    """) { data ->

    def (record, thing) = data.graph

    thing.remove('nationality')
    
    data.scheduleSave()
}
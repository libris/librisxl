/**
    Remove the following construct from all works. It is blocking work deduplication. 
    It is used to generate a 'd' in 006/23. It means the same thing as Text+Electronic and is redundant.  
 
         "hasPart": [
          {
            "@type": "Multimedia",
            "genreForm": [
              {
                "@id": "https://id.kb.se/marc/Document"
              }
            ]
          }
        ],
     
    See LXL-4201
 */

def where = """
    collection = 'bib' 
    AND data #>>'{@graph, 1, instanceOf, hasPart}' LIKE '%https://id.kb.se/marc/Document%'
    """

selectBySqlWhere(where) { bib ->
    def (_, thing) = bib.graph
    
    boolean changed = ((List<Map>) thing.instanceOf.hasPart).removeAll { part ->
        part.'@type' == 'Multimedia' && part.genreForm == [["@id": "https://id.kb.se/marc/Document"]]
    }
    
    if (changed) {
        if (thing.instanceOf.hasPart.isEmpty()) {
            thing.instanceOf.remove('hasPart')
        }
        bib.scheduleSave()
    }
}
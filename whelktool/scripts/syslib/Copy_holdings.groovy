String CURRENT_SIGEL = 'https://libris.kb.se/library/Ssb'
String NEW_SIGEL = 'https://libris.kb.se/library/Mlc'

File bibIDs = new File(scriptDir, "Copy_hold_bibID.txt")

def holdList = []

String bibidstring = bibIDs.readLines().join("','")

selectBySqlWhere("""id in

(
 select lh.id
  from
   lddb lb
  left join
   lddb lh
  on lh.data#>>'{@graph,1,itemOf,@id}' = lb.data#>>'{@graph,1,@id}'
 where lb.data#>>'{@graph,0,controlNumber}' in ( '$bibidstring' ) and lb.collection = 'bib'
 and
 lh.data#>>'{@graph,1,heldBy,@id}' = '{$CURRENT_SIGEL}'
 
 )""", silent: false, { hold ->

def holdData = hold.doc.data

holdList.add( create(holdData) )

    def heldBy = hold.graph[1].heldBy

    heldBy["@id"] = NEW_SIGEL

    if (hold.graph[1].hasComponent) {
        hold.graph[1].hasComponent.each { component ->
            if (component.heldBy) {
                component.heldBy["@id"] = NEW_SIGEL
            }
        }
    }
})

    selectFromIterable(holdList, { newlyCreatedItem ->
    newlyCreatedItem.scheduleSave()
 
  })

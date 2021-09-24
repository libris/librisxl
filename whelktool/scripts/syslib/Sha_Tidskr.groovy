PrintWriter noHold = getReportWriter("No-Holdings.txt")

File bibids = new File(scriptDir, "Sha_035_uppd.txt")
List<String> ProgramLines = bibids.readLines()

for (String operation : ProgramLines) {
String fuzzyID = operation.trim().toUpperCase()

if (fuzzyID ==~ /^\(\D{6}\)(.*)/) { // om ID börjar med parentes och kod, t.ex. (Libris)

String ctrlnr = fuzzyID.replaceFirst(/^\(\D{6}\)/, "") // strippa inledande parentesen och innehåll

where = """id in 
    (select lh.id
    from lddb lb
    left join lddb lh on lh.data#>>'{@graph,1,itemOf,@id}' = lb.data#>>'{@graph,1,@id}'
    where lb.collection = 'bib' and
    lb.data#>>'{@graph,0,controlNumber}' = '${ctrlnr}'
    and lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Sha'
)"""
}
else { // om det inte är ett kontrollnummer, sök istället med LibrisIII-nr

where = """id in
    (select lh.id
    from lddb lb
    left join lddb lh on lh.data#>>'{@graph,1,itemOf,@id}' = lb.data#>>'{@graph,1,@id}'
    where lb.collection = 'bib' and
    lb.data#>'{@graph,0,identifiedBy}' @> '[{\"@type\": \"LibrisIIINumber\", \"value\":\"$fuzzyID\"}]'
    and lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Sha'
)"""
}

boolean foundHold = false

selectBySqlWhere(where, silent: false, { hold ->
def items = hold.graph[1]
foundHold = true

  if (items.hasComponent) {
      items.remove('hasComponent') // ta bort nuvarande hasComponent om det finns
      
		items["hasComponent"] = []
		items["hasComponent"] << // lägg till korrekt hasComponent
     				
                                            [
                                                  "@type": "Item",
                                                  "physicalLocation": "Tidskr",
                                                  "heldBy": ["@id": "https://libris.kb.se/library/Sha"]
                                            ]
        				
   // } 

    }

  if (items.shelfMark) {
             items.remove('shelfMark')
      }

  if (items.shelfControlNumber) {
             items.remove('shelfControlNumber')
      }

 if (items.physicalLocation) { // Om physicalLocation finns, ändra till 'Tidskr'
                items.physicalLocation = 'Tidskr'
      }         
      
  if (!items.physicalLocation) { // Om physicalLocation inte finns, lägg till med värdet 'Tidskr'
                items.physicalLocation = 'Tidskr'
        }       

        hold.scheduleSave(loud: true, onError: { e ->
            failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
        })
})

if (!foundHold) {
     noHold.println("$fuzzyID not found.")
     }
}

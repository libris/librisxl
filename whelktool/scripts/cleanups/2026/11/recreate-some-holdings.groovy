KbSigel = ["https://libris.kb.se/library/S",
           "https://libris.kb.se/library/SRo"]

// från 2026-11-11 19:00 till 2026-11-13 13:00

String where =
        "collection = 'hold' AND data#>>'{@graph,1,heldBy,@id}' IN ( '£' ) and created > '2026-11-11 19:00:00.000+01' and created < '2026-11-13 13:00:00.000+01'"
                .replace("£", String.join("', '", KbSigel))

//String where = "id = 'lqxnr3tr19m73tl'" // qa record
//String where = "id = '0m17zzmwx53f7sqt'" // local

List graphLists = []
List itemList = []

// Side-store and delete
selectBySqlWhere(where) { data ->
    List rawGraphList = data.graph

    data.scheduleDelete(loud: true)
    graphLists.add(rawGraphList)
}

// Recreate items in memory
for (List rawGraphList : graphLists) {
    def item = create( [ "@graph" : rawGraphList] )
    itemList.add(item)
}

// Save new items
selectFromIterable(itemList, { newlyCreatedItem ->
    newlyCreatedItem.scheduleSave(loud: true)
})

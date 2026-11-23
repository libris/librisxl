// Use with care! *ALWAYS* run with plenty of memory, -Xmx4G at least and with --allow-loud --validation SKIP_AND_LOG
// and *DO NOT CANCEL* once started! If you cancel mid-run, the records might be deleted without being recreated, which is mess to sort out.

File holdsToRecreate = new File(scriptDir, "holdingsToRecreate.txt")

List graphLists = []
List itemList = []

// Side-store and delete
selectByIds( holdsToRecreate.readLines() ) { data ->

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

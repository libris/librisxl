String where = "data#>'{@graph,1,image}' is not null"

selectBySqlWhere(where) { doc ->
    doc.graph[1].remove("image")
    //System.err.println("After removing images: " + doc.graph[1])
    doc.scheduleSave()
}
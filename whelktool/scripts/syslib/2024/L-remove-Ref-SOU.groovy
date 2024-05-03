File bibIDs = new File(scriptDir, "L-SOU-ID.txt")
  
selectByIds( bibIDs.readLines() ) { hold ->
def item = hold.graph[1]

if (item["hasComponent"] && item["hasComponent"]?.size() > 1) {
    List component = item["hasComponent"]
    boolean change = false
    boolean modified = false

    component?.each { c ->
        if (c['shelfMark'] instanceof List) {
            def removedShelfMark = c['shelfMark'].removeAll { n ->
                asList(n['label']).any { it.contains('Ref') }
                change = true
            }
            if (c['shelfMark'].isEmpty()) {
                c.remove('shelfMark')
                change = true
            }
        } else if (c['shelfMark'] instanceof Map && asList(c['shelfMark']['label']).any { it.contains('Ref') }) {
                c.remove('shelfMark')
                change = true
        }

        if (change && !c['shelfMark']) {
        c.removeAll { it }
        modified = true
        }
    }

    if (modified) {
        component?.removeAll { !it }
        if (item["hasComponent"].isEmpty()) {
            item.remove('hasComponent')
        }
        hold.scheduleSave(loud: true)
    }

}
}

File GeHolds = new File(scriptDir, "GeHolds.txt")
selectByIds( GeHolds.readLines() ) { hold ->

 def items = hold.graph[1]['hasComponent']
 boolean changed = false
 
 hold.graph[1]['hasComponent']?.each { c ->
        if (c['physicalLocation'] instanceof List) {
                c.remove('physicalLocation')
                changed = true
        } else if (c['physicalLocation'] instanceof Map ) {
            c.remove('physicalLocation')
            changed = true
            }
        }
        
    def item = hold.graph[1]
    if (item["physicalLocation"]) {
     item.remove('physicalLocation')
     changed = true
 }
        
  if (changed) {
  hold.scheduleSave(loud: true)
  }
 
}

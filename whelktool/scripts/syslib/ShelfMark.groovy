File GeHolds = new File(scriptDir, "Bestand-id.txt")

selectByIds(GeHolds.readLines()) {hold ->
  def items = hold.graph[1]
  boolean changed = false

  if (items["shelfMark"]) {
    items.remove('shelfMark')
    changed = true
  }
  if (changed) {
    hold.scheduleSave(loud: true)
  }
}

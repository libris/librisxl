File bibIds = new File(scriptDir, "Romb_ids.txt")

selectByIds(bibIds.readLines()) {bib ->
  def record = bib.graph[0]

  if (record["bibliography"]) {
    record["bibliography"] <<
    [ "@id": "https://libris.kb.se/library/ROMB" ]
  }
  
  if (!record["bibliography"]) {
    record["bibliography"] = []
    record["bibliography"] <<
    [ "@id": "https://libris.kb.se/library/ROMB" ]
  }

  bib.scheduleSave(loud: true)
}
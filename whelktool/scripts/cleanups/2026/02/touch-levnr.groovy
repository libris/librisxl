
var ids = new URI("https://libris.kb.se/_bulk-change/reports/0lvdrm5hxn6nn7vb-2026-02-06T111857/MODIFIED.txt").toURL().readLines()

selectByIds(ids) { docItem ->
    docItem.scheduleSave(loud: true)
}
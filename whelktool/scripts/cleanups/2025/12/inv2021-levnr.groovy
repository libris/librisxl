import whelk.util.DocumentUtil
import whelk.util.Unicode

var before = "INV2021"
var after = "LEVNR"

var where = """
        collection = 'hold' AND
        data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/S'
        AND deleted = false
    """

selectBySqlWhere(where) { hold ->
    var main = hold.graph[1]
    var changed = DocumentUtil.traverse(main, { value, path ->
        if (path && path.last() == "shelfControlNumber" && value instanceof String) {
            String shelfControlNumber = (String) value
            if (shelfControlNumber.startsWith(before)) {
                return new DocumentUtil.Replace(after + Unicode.stripPrefix(shelfControlNumber, before))
            }
        }
    })

    if (changed) {
        hold.scheduleSave(loud: true)
    }
}
/**
 Remove leading zeroes in shelfControlNumber LEVNR
 e.g. LEVNR-00145000 -> LEVNR-145000

 See TB-7904
 */

import whelk.util.DocumentUtil
import java.util.regex.Pattern

var LEVNR = "LEVNR-"
var pattern = Pattern.compile(LEVNR + "0+(\\d+)")


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
            var m = pattern.matcher(shelfControlNumber)
            if (m.matches()) {
                return new DocumentUtil.Replace(LEVNR + m.group(1))
            }
        }
    })

    if (changed) {
        hold.scheduleSave(loud: true)
    }
}
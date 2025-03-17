import whelk.Document
import whelk.Whelk
import whelk.datatool.form.ModifiedThing
import whelk.datatool.form.Transform

import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.RECORD_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.TARGET_FORM_KEY

Map matchForm = parameters.get(MATCH_FORM_KEY)
Map targetForm = parameters.get(TARGET_FORM_KEY)

Transform transform = new Transform(matchForm, targetForm, getWhelk())

selectByForm(transform.matchForm) {
    try {
        if (modify(transform, it.doc, it.whelk)) {
            it.scheduleSave(loud: isLoudAllowed)
        }
    } catch (ModifiedThing.IllegalModificationException ignored) {
    }
}

private static boolean modify(Transform tf, Document doc, Whelk whelk) {
    Map thing = doc.getThing()
    thing.put(RECORD_KEY, doc.getRecord())

    var m = new ModifiedThing(thing, tf, whelk.getJsonld().repeatableTerms)

    Map after = new HashMap<>(m.after)

    ((List) doc.data.get(GRAPH_KEY)).set(0, after.remove(RECORD_KEY))
    ((List) doc.data.get(GRAPH_KEY)).set(1, after)

    return m.isModified()
}


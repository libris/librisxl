import whelk.Document
import whelk.datatool.form.MatchForm

import static whelk.JsonLd.RECORD_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY

Map matchForm = parameters.get(MATCH_FORM_KEY)

MatchForm mf = new MatchForm(matchForm, getWhelk())

selectByForm(mf) {
    if(mf.matches(getFramedThing(it.doc))) {
        it.scheduleDelete(loud: isLoudAllowed)
    }
}

private static Map getFramedThing(Document doc) {
    Map<String, Object> thing = doc.clone().getThing();
    thing.put(RECORD_KEY, doc.getRecord());
    return thing
}
import whelk.Document
import whelk.datatool.form.MatchForm

import static whelk.exception.LinkValidationException.IncomingLinksException
import static whelk.JsonLd.RECORD_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY

PrintWriter failed = getReportWriter("failed-to-delete.txt")

Map matchForm = parameters.get(MATCH_FORM_KEY)

MatchForm mf = new MatchForm(matchForm, getWhelk())

selectByForm(mf) {
    if(mf.matches(getFramedThing(it.doc))) {
        it.scheduleDelete(loud: isLoudAllowed, onError: { e ->
            if (e instanceof IncomingLinksException) {
                failed.println("Failed to delete $it.doc.shortId: ${e.getMessage()}")
            } else {
                throw e
            }
        })
    }
}

private static Map getFramedThing(Document doc) {
    Map<String, Object> thing = doc.clone().getThing();
    thing.put(RECORD_KEY, doc.getRecord());
    return thing
}
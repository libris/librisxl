import whelk.datatool.bulkchange.Specification

import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY

Map matchForm = parameters.get(MATCH_FORM_KEY)

Specification.Delete delete = new Specification.Delete(matchForm)

selectByForm(matchForm) {
    if(delete.matches(it.doc, it.whelk)) {
        it.scheduleDelete(loud: isLoudAllowed)
    }
}
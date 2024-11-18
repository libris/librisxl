import whelk.datatool.bulkchange.Specification

import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.TARGET_FORM_KEY

Map matchForm = parameters.get(MATCH_FORM_KEY)
Map targetForm = parameters.get(TARGET_FORM_KEY)

Specification.Update update = new Specification.Update(matchForm, targetForm)
List<String> ids = update.findIds(getWhelk())

selectByIds(ids) {
    if(update.modify(it.doc, it.whelk)) {
        it.scheduleSave(loud: isLoudAllowed)
    }
}
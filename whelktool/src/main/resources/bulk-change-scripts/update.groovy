import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.TARGET_FORM_KEY

Map matchForm = parameters.get(MATCH_FORM_KEY)
Map targetForm = parameters.get(TARGET_FORM_KEY)

selectByForm(matchForm) { doc ->
    if(doc.modify(matchForm, targetForm)) {
        doc.scheduleSave(loud: isLoudAllowed)
    }
}
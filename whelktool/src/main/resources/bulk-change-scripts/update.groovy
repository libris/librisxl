import static whelk.datatool.bulkchange.Bulk.Other.matchForm
import static whelk.datatool.bulkchange.Bulk.Other.targetForm

Map matchForm = parameters.get(matchForm)
Map targetForm = parameters.get(targetForm)

selectByForm(matchForm) { doc ->
    if(doc.modify(matchForm, targetForm)) {
        doc.scheduleSave(loud: isLoudAllowed)
    }
}
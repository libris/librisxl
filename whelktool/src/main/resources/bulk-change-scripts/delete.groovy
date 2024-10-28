import static whelk.datatool.bulkchange.Bulk.Other.matchForm
import static whelk.datatool.bulkchange.Bulk.Other.targetForm

Map matchForm = parameters.get(matchForm)

selectByForm(matchForm) { doc ->
    if(doc.matches(matchForm)) { // FIXME implement documentItem::matches
        doc.scheduleDelete(loud: isLoudAllowed)
    }
}
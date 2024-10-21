import static whelk.datatool.bulkchange.BulkChange.Prop.matchForm
import static whelk.datatool.bulkchange.BulkChange.Prop.targetForm

Map matchForm = parameters.get(matchForm)
Map targetForm = parameters.get(targetForm)

selectByForm(matchForm) { doc ->
    if(doc.modify(matchForm, targetForm)) {
        doc.scheduleSave(loud: isLoudAllowed)
    }
}
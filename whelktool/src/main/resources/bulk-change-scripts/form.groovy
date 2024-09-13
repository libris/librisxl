import static whelk.datatool.bulkchange.BulkChange.Prop.matchForm
import static whelk.datatool.bulkchange.BulkChange.Prop.targetForm

// TODO
println("Hello from script! " + parameters.get(matchForm))

Map matchForm = parameters.get(matchForm)
Map targetForm = parameters.get(targetForm)

selectByForm(matchForm) { doc ->
    doc.modify(matchForm, targetForm)
    doc.scheduleSave(loud: isLoudAllowed)
}
import static whelk.datatool.bulkchange.BulkChange.Prop.matchForm

Map matchForm = parameters.get(matchForm)

selectByForm(matchForm) { doc ->
    if(doc.matches(matchForm)) {
        doc.scheduleDelete(loud: isLoudAllowed)
    }
}

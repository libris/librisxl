import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY

Map matchForm = parameters.get(MATCH_FORM_KEY)

selectByForm(matchForm) { doc ->
    if(doc.matches(matchForm)) { // FIXME implement documentItem::matches
        doc.scheduleDelete(loud: isLoudAllowed)
    }
}
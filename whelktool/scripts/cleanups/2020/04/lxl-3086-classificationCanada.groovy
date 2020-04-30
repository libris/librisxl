PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>'{@graph,2,marc:hasClassificationNumbersAssignedInCanada}' is not null"

selectBySqlWhere(where) { data ->
    def (record, instance, work) = data.graph

    // force @graph,1,classification to be a list
    if (instance["classification"] != null) {
        if (!(instance["classification"] instanceof List))
            instance["classification"] = [instance["classification"]]
    } else
        instance["classification"] = []

    // Copy the work classifications
    instance["classification"].addAll( getObjectList(work["marc:hasClassificationNumbersAssignedInCanada"]) )

    // Ditch the work classifications
    work.remove("hasClassificationNumbersAssignedInCanada")

    // Clean up classifications
    for (Object classification : instance["classification"]) {
        if ( !(classification instanceof Map) )
            continue

        classification.remove("marc:existenceInLacCollection")
        classification.remove("marc:sourceOfCallClassNumber")

        if (classification["marc:itemNumber"] != null) {
            classification["itemPortion"] = classification["marc:itemNumber"]
            classification.remove("marc:itemNumber")
        }

        if (classification["marc:classificationNumber"] != null) {
            classification["classificationPortion"] = classification["marc:classificationNumber"]
            classification.remove("marc:classificationNumber")
        }

        if (classification["marc:typeCompletenessSourceOfClassCallNumber"] != null && (
                classification["marc:typeCompletenessSourceOfClassCallNumber"] == "marc:LcBasedCallNumberAssignedByLac" ||
                        classification["marc:typeCompletenessSourceOfClassCallNumber"] == "marc:CompleteLcClassNumberAssignedByLac" ||
                        classification["marc:typeCompletenessSourceOfClassCallNumber"] == "marc:IncompleteLcClassNumberAssignedByLac" )) {
            classification["source"] = classification["marc:typeCompletenessSourceOfClassCallNumber"]
        }
        classification.remove("marc:typeCompletenessSourceOfClassCallNumber")
    }

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}

List getObjectList(Object data) {
    if (data instanceof List)
        return data
    if (data instanceof Map)
        return [data]
    else throw new RuntimeException("Expected list/map, found: " + data.getClass().toString())
}
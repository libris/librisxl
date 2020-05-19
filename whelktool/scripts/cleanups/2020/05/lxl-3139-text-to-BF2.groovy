PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>>'{@graph,1,@type}' = 'TextInstance' and data#>>'{@graph,2,@type}' = 'Text'"

selectBySqlWhere(where) { data ->
    def (record, instance, work) = data.graph
    boolean changed = false

    if (instance["marc:mediaTerm"] == null) {
        List carrierTypes = instance["carrierType"]
        for (Object carrierType : carrierTypes) {
            if (carrierType["@id"] == "https://id.kb.se/marc/RegularPrint" ||
                    carrierType["@id"] == "https://id.kb.se/marc/RegularPrintReproduction" ||
                    carrierType["@id"] == "https://id.kb.se/term/rda/Volume" ||
                    carrierType["@id"] == "https://id.kb.se/marc/LargePrint" ||
                    carrierType["@id"] == "https://id.kb.se/marc/RegularPrintReproductionEyeReadablePrint") {
                instance["@type"] = "Print"
                changed = true
            }
        }
    }

    else if (instance["marc:mediaTerm"].contains("Elektronisk") || instance["marc:mediaTerm"].contains("Electronic")) {
        instance["@type"] = "Electronic"
        changed = true
    }

    else if (instance["marc:mediaTerm"].contains("handskrift") || instance["marc:mediaTerm"].contains("Handskrift")) {
        instance["@type"] = "Manuscript"
        changed = true
    }

    else if (instance["marc:mediaTerm"].contains("Kombinerat") || instance["marc:mediaTerm"].contains("kombinerat")) {
        instance["@type"] = "Instance"
        changed = true
    }

    else if (instance["hasPart"]) {
        for (Object part : instance["hasPart"]) {
            if (part["@type"] == "TextInstance" && part["carrierType"]) {
                for (Object carrierType : part["carrierType"]) {
                    if (carrierType["@id"] == "https://id.kb.se/marc/RegularPrint") {
                        part["@type"] = "Print" // <- Careful review point! Is this a correct interpretation of the spec?
                        changed = true
                    }
                }
            }
        }
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

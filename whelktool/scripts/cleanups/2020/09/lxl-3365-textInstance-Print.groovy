/*
select collection = 'bib'

if '{@graph,1,@type}' = 'TextInstance'
  'TextInstance' = 'Print'

if '{@graph,hasPart,@type}' = 'TextInstance' and '{@graph,hasPart,carrierType}' = 'https://id.kb.se/marc/RegularPrint'
  'TextInstance' = 'Print'
else
  'TextInstance' = 'Instance'

 */
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection='bib' and data::text like '%TextInstance%'"

selectBySqlWhere(where) { data ->
    boolean changed = false

    def instance = data.graph[1]

    if (instance["@type"] == "TextInstance") {
        instance["@type"] = "Print"
        changed = true
    }

    if (instance.hasPart) {
        for (Map part : instance.hasPart) {
            if (part["@type"] == "TextInstance") {
                if (part.carrierType) {
                    for (Map ct : part.carrierType) {
                        if (ct["@id"] == "https://id.kb.se/marc/RegularPrint") {
                            part["@type"] = "Print"
                            changed = true
                        }
                    }
                } else {
                    part["@type"] = "Instance"
                    changed = true
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
/*
select collection='bib' and '{@graph,1,instanceOf,@type}' = 'ManuscriptNotatedMusic'

if missing add "https://id.kb.se/term/saogf/Handskrifter" to genreForm list in 'instanceOf'
  "genreForm": [
    {
      "@id": "https://id.kb.se/term/saogf/Handskrifter"
    }
  ]

if  ('{@graph,1,@type}' = 'Instance'

  if "suitableForElectronic"
    '{@graph,1,@type}' = 'Electronic'
  else
    '{@graph,1,@type}' = 'Manuscript'

'{@graph,1,instanceOf,@type}' = 'NotatedMusic'

 */
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection='bib' and data#>>'{@graph,1,instanceOf,@type}' = 'ManuscriptNotatedMusic'"

selectBySqlWhere(where) { data ->
    def instance = data.graph[1]

    if (instance.instanceOf.genreForm == null) {
        instance.instanceOf["genreForm"] = []
    }
    boolean alreadyHasGF = false
    instance.instanceOf.genreForm.each { gf ->
        if (gf["@id"] == "https://id.kb.se/term/saogf/Handskrifter")
            alreadyHasGF = true
    }
    if (!alreadyHasGF) {
        instance.instanceOf["genreForm"].add(["@id":"https://id.kb.se/term/saogf/Handskrifter"])
    }

    if (instance["@type"] == "Instance") {
        if (suitableForElectronic(instance)) {
            instance["@type"] = "Electronic"
        } else {
            instance["@type"] = "Manuscript"
        }
    }

    instance.instanceOf["@type"] = "NotatedMusic"

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}

boolean suitableForElectronic(instance) {
    if (instance["marc:mediaTerm"] && instance["marc:mediaTerm"].matches("^ele[ck]tron")) {
        return true
    }

    if (instance.carrierType) {
        for (Map ct : instance.carrierType) {
            switch (ct["@id"]) {
                case "https://id.kb.se/marc/DirectElectronic":
                case "https://id.kb.se/marc/Electronic":
                case "https://id.kb.se/marc/Online":
                case "https://id.kb.se/marc/OnlineResource":
                case "https://id.kb.se/marc/OpticalDisc":
                case "https://id.kb.se/term/rda/ComputerDisc":
                case "https://id.kb.se/term/rda/OnlineResource":
                case "https://id.kb.se/term/rda/MicrofilmReel":
                case "https://id.kb.se/term/rda/Microfiche":
                case "https://id.kb.se/marc/Microfiche":
                case "https://id.kb.se/marc/Microfilm":
                case "https://id.kb.se/marc/Microopaque":
                    return true
            }
        }
    }

    return false
}

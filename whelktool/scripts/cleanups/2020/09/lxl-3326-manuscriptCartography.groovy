/*
select collection='bib' and '{@graph,1,instanceOf,@type}' = 'ManuscriptCartography'

if '{@graph,1,instanceOf,genreForm}' = 'https://id.kb.se/term/gmgpc/swe/Handritade%20kartor'

    '{@graph,1,instanceOf,@type}' == 'Cartography'

if graph,1,@type = 'Instance'
  if '{@graph,1,marc:mediaTerm}' LIKE 'ele[ck]tron%' OR '{@graph,1,carrierType}' LIKE
    carrierTypeMap = [
    "https://id.kb.se/marc/DirectElectronic": ELECTRONIC,
    "https://id.kb.se/marc/Electronic": ELECTRONIC,
    "https://id.kb.se/marc/Online": ELECTRONIC,
    "https://id.kb.se/marc/OnlineResource": ELECTRONIC,
    "https://id.kb.se/marc/OpticalDisc": ELECTRONIC,
    "https://id.kb.se/term/rda/ComputerDisc": ELECTRONIC,
    "https://id.kb.se/term/rda/OnlineResource": ELECTRONIC,
    "https://id.kb.se/term/rda/MicrofilmReel": ELECTRONIC,
    "https://id.kb.se/term/rda/Microfiche": ELECTRONIC,
    "https://id.kb.se/marc/Microfiche": ELECTRONIC,
    "https://id.kb.se/marc/Microfilm": ELECTRONIC,
    "https://id.kb.se/marc/Microopaque": ELECTRONIC,
    ]

    '{@graph1,@type}' ==  'Electronic'

  else

    graph,1,@type == 'Manuscript'
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection='bib' and data#>>'{@graph,1,instanceOf,@type}' = 'ManuscriptCartography'"

selectBySqlWhere(where) { data ->
    def instance = data.graph[1]

    boolean changed = false

    instance.instanceOf.genreForm.each{ gf ->
        if (gf["@id"] == "https://id.kb.se/term/gmgpc/swe/Handritade%20kartor") {
            instance.instanceOf["@type"] = "Cartography"
            changed = true
        }
    }

    if (instance["@type"] == "Instance") {
        if (suitableForElectronic(instance)) {
            instance["@type"] = "Electronic"
        } else {
            instance["@type"] = "Manuscript"
        }
        changed = true
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

boolean suitableForElectronic(instance) {
    if (instance["marc:mediaTerm"].matches("^ele[ck]tron.?\$")) {
        return true
    }

    instance.carrierType.each { ct ->
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

    return false
}
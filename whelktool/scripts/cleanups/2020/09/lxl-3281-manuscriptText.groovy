/*
ELECTRONIC = 'Electronic'
MANUSCRIPT = 'Manuscript'
TEXT = 'Text'

select collection='bib' and '{@graph,1,instanceOf,@type}' = 'ManuscriptText'

if missing add "https://id.kb.se/term/saogf/Handskrifter" to genreForm list in 'instanceOf' :

  "genreForm": [
    {
      "@id": "https://id.kb.se/term/saogf/Handskrifter"
    }
  ]

if  ('{@graph,1,@type}' = 'TextInstance' OR 'Instance') AND '{@graph,1,marc:mediaTerm}' LIKE '%handskrift%

  '{@graph,@type}' == MANUSCRIPT
  '{@graph,1,instanceOf,@type}' == TEXT

if '{@graph,1,marc:mediaTerm}' LIKE 'ele*tron%' OR  '{@graph,1,carrierType}'

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

  '{@graph,@type}' == ELECTRONIC
  '{@graph,1,instanceOf,@type}' == TEXT

else

  '{@graph,1,instanceOf,@type}' = TEXT
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection='bib' and data#>>'{@graph,1,instanceOf,@type}' = 'ManuscriptText'"

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

    if (instance["@type"] == "TextInstance" || instance["@type"] == "Instance") {
        if (suitableForElectronic(instance)) {
            instance["@type"] = "Electronic"
        } else {
            instance["@type"] = "Manuscript"
        }
    }

    instance.instanceOf["@type"] = "Text"

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
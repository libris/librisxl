PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = """
id in (
  select lh.id
  from
    lddb lh
  left join lddb lb
    on lh.data#>>'{@graph,1,itemOf,@id}' = lb.data#>>'{@graph,1,@id}'
  where
    lh.collection = 'hold' and
    lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Udix' and
    lb.collection = 'bib' and
    lb.data#>>'{@graph,1,@type}' <> 'Electronic'
)
"""

selectBySqlWhere(where) { hold ->

    boolean bibIsConsideredPrint = false
    selectBySqlWhere( "id in (select dependsonid from lddb__dependencies where id = '" + hold.doc.getId() + "')") { bib ->
        def instance = bib.graph[1]
        if (!suitableForElectronic(instance)) {
            bibIsConsideredPrint = true
        }
    }

    if (bibIsConsideredPrint) {
        System.out.println("Was about to delete holding: " + hold.doc.getURI())
        /*
        scheduledForUpdating.println("${hold.doc.getURI()}")
        hold.scheduleDelete(onError: { e ->
            failedUpdating.println("Failed to delete ${hold.doc.shortId} due to: $e")
        })*/
    }

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

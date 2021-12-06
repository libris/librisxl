PrintWriter removedLink = getReportWriter("removed-otherPhysicalFormat-in-original.txt")
PrintWriter addedLink = getReportWriter("added-reproductionOf-in-digital.txt")

String where = """
    collection = 'bib'
    AND data#>'{@graph,1,otherPhysicalFormat}' IS NOT NULL
    AND data#>'{@graph,1,production}' IS NOT NULL
    AND data#>'{@graph,0,bibliography}' @> '[{"@id":"https://libris.kb.se/library/DIGI"}]'
"""

selectBySqlWhere(where) { data ->
    Map mainEntity = data.graph[1]
    String controlNumber = data.graph[0].controlNumber

    // Avoid ambiguity
    if (!(mainEntity.otherPhysicalFormat?.size() == 1 && mainEntity.production?.size() == 1))
        return

    Map otherPhysicalFormat = mainEntity.otherPhysicalFormat[0]
    Map production = mainEntity.production[0]

    String displayText = otherPhysicalFormat['marc:displayText'] in List
            ? otherPhysicalFormat['marc:displayText'][0]
            : otherPhysicalFormat['marc:displayText']

    String typeNote = production.typeNote

    if (!(displayText =~ /[Oo]ri?ginal ?version/ && typeNote =~ /Digitalt faksimil/ && otherPhysicalFormat.describedBy))
        return

    String controlNumberForPhysical = otherPhysicalFormat.describedBy[0].controlNumber
    String idForPhysical

    selectBySqlWhere("collection = 'bib' AND data#>>'{@graph,0,controlNumber}' = '${controlNumberForPhysical}'") { physical ->
        Map mainEntityPhysical = physical.graph[1]
        idForPhysical = mainEntityPhysical['@id']

        boolean removed = mainEntityPhysical.otherPhysicalFormat?.removeAll {
            it.describedBy && it.describedBy[0].controlNumber == controlNumber
        }
        if (removed) {
            if (mainEntityPhysical.otherPhysicalFormat.isEmpty())
                mainEntityPhysical.remove('otherPhysicalFormat')
            removedLink.println(physical.doc.shortId)
            physical.scheduleSave()
        }
    }

    if (idForPhysical && !mainEntity.reproductionOf) {
        mainEntity['reproductionOf'] = ['@id': idForPhysical]
        addedLink.println(data.doc.shortId)
        data.scheduleSave()
    }
}

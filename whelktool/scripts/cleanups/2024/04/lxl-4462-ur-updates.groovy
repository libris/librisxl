import whelk.util.DocumentUtil

/*
 * For all records in bibliography UR
 * - Move hasTitle.subtitle to hasTitle.hasPart.partName
 * - For all holdings: remove all marc:publicNote matching "YYYYMYYMDD - YYYYMMDD"
 *
 * See LXL-4462 for more info.
 *
 */

alreadyHadTitlePart = getReportWriter("already-had-title-part.txt")

String where = """
  collection = 'bib' 
  AND deleted = false
  AND data#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/UR"}]'
"""

selectBySqlWhere(where) { bib ->
    def (_, mainEntity) = bib.graph
    asList(mainEntity['hasTitle']).each { Map title ->
        if (title['@type'] == 'Title' && title.subtitle) {
            if (title.hasPart) {
                alreadyHadTitlePart.println(mainEntity['@id'])
                return
            }

            title['hasPart'] = [
                '@type'    : 'TitlePart',
                'partName' : title.remove('subtitle')
            ]

            bib.scheduleSave(loud: true)
        }
    }

    updateHolds(mainEntity['@id'])
}

void updateHolds(String bibId) {
    String where = "collection = 'hold' and data#>>'{@graph,1,itemOf,@id}' = '${bibId}'"

    selectBySqlWhere(where) { hold ->
        var modified = DocumentUtil.findKey(hold.graph, 'marc:publicNote') { value, path ->
            List notes = asList(value).findAll { !(it ==~ /\d{8} - \d{8}/) }
            return notes != value
                ? new DocumentUtil.Replace(notes)
                : DocumentUtil.NOP
        }

        if (modified) {
            hold.scheduleSave(loud: true)
        }
    }
}
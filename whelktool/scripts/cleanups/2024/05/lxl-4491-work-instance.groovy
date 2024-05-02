import whelk.datatool.DocumentItem

skipped = getReportWriter("skipped")

String where = "id IN (SELECT id FROM lddb__identifiers WHERE graphindex = 1 AND mainid IS true AND iri LIKE '%#work')"

selectBySqlWhere(where) { DocumentItem bib ->
    def (record, work, instance) = bib.graph;

    if (record && work && instance) {
        record['mainEntity']['@id'] = instance['@id']
        workId = work.remove('@id')
        work.remove('sameAs')
        instance['instanceOf'] = work
        instance['sameAs'] = asList(instance['sameAs']) + [['@id': workId]]
        bib.doc.data['@graph'] = [record, instance]
        bib.scheduleSave(loud: true)
    } else {
        skipped.println(bib.doc.shortId)
    }
}
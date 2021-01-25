PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

File bibids = new File(scriptDir, 'bibIDs')

for (String bibId : bibids.readLines()) {
    String where
    if (bibId.matches("^[0-9]{0,13}\$"))
        where = "id in (select id from lddb__identifiers where iri = 'http://libris.kb.se/bib/$bibId')"
    else
        where = "id in (select id from lddb where id = '$bibId')" // yeew..

    selectBySqlWhere(where) { data ->
        boolean changed = false

        def (record, instance) = data.graph

        instance.publication.each { pub ->
            if (! (pub instanceof Map))
                return
            if (pub.country == null)
                return

            if (pub.country instanceof List) {
                for (Object country : pub.country) {
                    if (!country instanceof Map)
                        continue
                    if (country.get("@id") == "https://id.kb.se/country/se") {
                        country.put("@id", "https://id.kb.se/country/sw")
                        changed = true
                    }
                }
            } else if (pub.country instanceof Map) {
                if (!pub.country instanceof Map)
                    return
                if (pub.country.get("@id") == "https://id.kb.se/country/se") {
                    pub.country.put("@id", "https://id.kb.se/country/sw")
                    changed = true
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
}
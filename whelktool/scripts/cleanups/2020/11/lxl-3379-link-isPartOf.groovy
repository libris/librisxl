/*
 This script is a partial copy of: librisxl/whelktool/scripts/2020/link-local-things/script.groovy

 The intended purpose is to link up "isPartOf" relations.
 */

selectBySqlWhere("""
    data#>>'{@graph,1,isPartOf}' LIKE '%"controlNumber":%'
    AND collection = 'bib'
""") { data ->
    boolean changed = false

    def instance = data.graph[1]

    asList(instance["isPartOf"]).each { part ->
        asList(part["describedBy"]).each { describedBy ->
            if (describedBy instanceof Map &&
                    describedBy["controlNumber"] &&
                    describedBy["controlNumber"] instanceof String &&
                    !describedBy["@id"]) {
                String controlNumber = sanitize(describedBy["controlNumber"])
                String properUri = findMainEntityId(controlNumber)

                if (properUri != null) {
                    //System.out.println("Replacing: " + describedBy + " with: " + ["@id":properUri])
                    describedBy.clear()
                    describedBy["@id"] = properUri
                    changed = true
                }
            }
        }
    }

    if (changed) {
        data.scheduleSave()
    }
}

String findMainEntityId(String ctrlNumber) {
    String mainId = null
    try {
        mainId = findCanonicalId("${baseUri.resolve(ctrlNumber)}#it")
    } catch (IllegalArgumentException e) {
    }
    if (mainId) {
        return mainId
    }
    def legacyId = "http://libris.kb.se/resource/bib/${ctrlNumber}"
    mainId = findCanonicalId(legacyId)
    if (mainId) {
        return mainId
    }

    def byLibris3Ids = []
    // IMPORTANT: This REQUIRES an index on '@graph[0]identifiedBy*.value'.
    // If that is removed, this slows to a GLACIAL crawl!
    ctrlNumber = ctrlNumber.replaceAll(/['"\\]/, '')
    selectBySqlWhere("""
    data #> '{@graph,0,identifiedBy}' @> '[{"@type": "LibrisIIINumber", "value": "${ctrlNumber}"}]'::jsonb and collection = 'bib'
    """, silent: true) {
        List mainEntityIDs = it.doc.getThingIdentifiers()
        if (mainEntityIDs.size() > 0)
            byLibris3Ids << mainEntityIDs[0]
    }
    if (byLibris3Ids.size() == 1) {
        return byLibris3Ids[0]
    }
    return null
}

String sanitize(String value) {
    return value.replaceAll(/\9/, '')
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}

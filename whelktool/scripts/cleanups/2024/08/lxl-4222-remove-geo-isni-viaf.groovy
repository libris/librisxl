/*
    Remove ISNI and VIAF identifiers from all :Geographic and cpmplex :Geograpic subjects.
    They probably belong to Jurisdictions with the same name as the Geographic place.

    See LXL-4222 for more information
 */

selectByCollection("auth") { doc ->
    if (getAtPath(doc.graph, [1, '@type']) == 'Geographic'
            || getAtPath(doc.graph, [1, 'termComponentList', 0, '@type']) == 'Geographic') {

        boolean modified = asList(getAtPath(doc.graph, [1, 'identifiedBy'])).removeAll {
            it['@type'] == 'VIAF' || it['@type'] == 'ISNI'
        }

        if (modified) {
            if (asList(getAtPath(doc.graph, [1, 'identifiedBy'])).isEmpty()) {
                doc.graph[1].remove('identifiedBy')
            }

            doc.scheduleSave()
        }
    }
}

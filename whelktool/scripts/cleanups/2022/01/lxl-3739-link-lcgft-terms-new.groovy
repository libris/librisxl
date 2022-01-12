/*
See LXL-3739 for more info.
*/

import whelk.util.DocumentUtil

selectByCollection('auth') { auth ->
    def data = auth.doc.data

    if (!data['@graph'][1].inScheme?.'@id'?.equals("https://id.kb.se/term/saogf")) {
        return
    }
    DocumentUtil.traverse(auth.doc.data, { value, path ->
        if (value instanceof Map && value.inScheme?.'@id'?.equals("https://id.kb.se/term/lcgft") && value.prefLabel
        && value.'@type'?.equals("GenreForm") && value.uri) {
            incrementStats(value.prefLabel, value.uri)
            auth.scheduleSave()
            return new DocumentUtil.Replace(['@id': value.uri])
        }
    })
}

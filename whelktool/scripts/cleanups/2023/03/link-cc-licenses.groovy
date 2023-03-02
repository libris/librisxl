/*
select distinct ?uri (count(?g) as ?count) (sample(?g) as ?sample) {
    graph ?g {?s kbv:uri ?uri . filter(REGEX(?uri, "https?://creativecommons.org/")) }
} order by desc(?count)
*/
import groovy.transform.Memoized

import whelk.Document
import whelk.util.DocumentUtil

URI_KEY = 'uri'

selectBySqlWhere("collection = 'bib' and data::text LIKE '%://creativecommons.org/%'") { bib ->
    var data = bib.doc.data
    DocumentUtil.traverse(data, { value, path ->
        if (!path || path[-1] != URI_KEY) {
            return
        }
        def uri = value instanceof List && value.size() > 0 ? value[0] : value
        if (uri instanceof String && uri.indexOf('://creativecommons.org/') > -1) {
            var licenseId = getLicenseId(uri)
            if (licenseId) {
                def owner = DocumentUtil.getAtPath(data, path[0..-2])
                assert owner instanceof Map && owner[URI_KEY].is(value)
                owner.clear()
                owner[ID] = licenseId
                bib.scheduleSave()
            }
        }
    })
}

@Memoized
String getLicenseId(String id) {
    id = id.replace(/\/licenses\/([123]\.[0-9])(\/.*)?/, '/licenses/4.0/')
    if (!id.endsWith('/')) {
        id += '/'
    }
    return findCanonicalId(id)
}

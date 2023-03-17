/*
For given paths, change property 'marc:version' to 'version' in bib and auth records.

See https://jira.kb.se/browse/LXL-1556 for more info.
*/

import whelk.datatool.DocumentItem

changed = getReportWriter("changed.tsv")
conflict = getReportWriter("conflict.tsv")

VERSION = 'version'
MARC_VERSION = 'marc:version'

// local paths from mainEntity (@graph, 1)
MARC_VERSION_BIB_PATHS =
        [
                ['instanceOf'],
                ['instanceOf', 'expressionOf'],
                ['instanceOf', 'hasPart'],
                ['instanceOf', 'relationship', '*', 'entity'],
                ['instanceOf', 'subject'],
                ['instanceOf', 'seriesMembership', 'inSeries', 'instanceOf']
        ]

MARC_VERSION_AUTH_PATHS =
        [
                [],
                ['hasVariant'],
                ['relatedTo'],
                ['subject'],
                ['marc:hasEstablishedHeadingLinkingEntryUniformTitle']
        ]

selectBySqlWhere("collection = 'bib' and deleted = false and data::text LIKE '%\"marc:version\"%'") { DocumentItem docItem ->
    oldToNew(docItem, MARC_VERSION_BIB_PATHS, MARC_VERSION, VERSION)
}

selectBySqlWhere("collection = 'auth' and deleted = false and data::text LIKE '%\"marc:version\"%'") { DocumentItem docItem ->
    oldToNew(docItem, MARC_VERSION_AUTH_PATHS, MARC_VERSION, VERSION)
}

def oldToNew(DocumentItem docItem, List<List<String>> paths, String oldProp, String newProp) {
    Map mainEntity = docItem.graph[1]
    String id = docItem.doc.shortId

    paths.each { p ->
        asList(getAtPath(mainEntity, p)).each { obj ->
            if (obj instanceof Map && obj[oldProp]) {
                String prettyPath = (['mainEntity'] + p).join('.')
                if (obj[newProp]) {
                    conflict.println([id, oldProp, newProp, prettyPath].join('\t'))
                    return
                }
                obj[newProp] = obj[oldProp]
                obj.remove(oldProp)
                docItem.scheduleSave()
                changed.println([id, oldProp, newProp, obj[newProp], prettyPath].join('\t'))
            }
        }
    }
}
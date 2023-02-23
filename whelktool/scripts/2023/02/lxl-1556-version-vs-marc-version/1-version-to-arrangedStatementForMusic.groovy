/*
Correct misuse of property 'version' in bib and auth records.

For given paths, change 'version' to 'marc:arrangedStatementForMusic' if the work type (in bib records) is any of
MovingImage, Music, Audio, NotatedMusic.

See https://jira.kb.se/browse/LXL-1556 for more info.
*/

import whelk.datatool.DocumentItem

changed = getReportWriter("changed.tsv")
conflict = getReportWriter("conflict.tsv")
likelyCorrectVersion = getReportWriter("likely-correct-version.txt")

VERSION = 'version'
MARC_ARRANGED = 'marc:arrangedStatementForMusic'

// local paths from mainEntity (@graph, 1)
VERSION_BIB_PATHS =
        [
                ['instanceOf'],
                ['instanceOf', 'expressionOf'],
                ['instanceOf', 'hasPart'],
                ['instanceOf', 'relationship', '*', 'entity']
        ]

VERSION_AUTH_PATHS =
        [
                [],
                ['relatedTo']
        ]

OK_WORK_TYPES = ['MovingImage', 'Music', 'Audio', 'NotatedMusic']

selectBySqlWhere("collection = 'bib' and deleted = false and data::text LIKE '%\"version\"%'") { DocumentItem docItem ->
    if (workType(docItem) in OK_WORK_TYPES) {
        oldToNew(docItem, VERSION_BIB_PATHS, VERSION, MARC_ARRANGED)
    }
}

selectBySqlWhere("collection = 'auth' and deleted = false and data::text LIKE '%\"version\"%'") { DocumentItem docItem ->
    oldToNew(docItem, VERSION_AUTH_PATHS, VERSION, MARC_ARRANGED)
}

def oldToNew(DocumentItem docItem, List<List<String>> paths, String oldProp, String newProp) {
    Map mainEntity = docItem.graph[1]
    String id = docItem.doc.shortId

    paths.each { p ->
        asList(getAtPath(mainEntity, p)).each { obj ->
            if (obj instanceof Map && obj[oldProp]) {
                String prettyPath = (['mainEntity'] + p).join('.')
                if (oldProp == VERSION && looksLikeDate(obj[VERSION])) {
                    likelyCorrectVersion.println([id, obj[VERSION], prettyPath].join('\t'))
                    return
                }
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

def workType(DocumentItem docItem) {
    return docItem.graph[1].instanceOf?.'@type'
}

def looksLikeDate(o) {
    asList(o).any { String s -> s =~ /\d{4}/ }
}
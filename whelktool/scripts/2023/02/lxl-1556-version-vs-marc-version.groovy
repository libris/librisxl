/*
Correct misuse of properties version and marc:version in bib and auth records.

For given paths change:
(in the following order, important!)
1. version -> marc:arrangedStatementForMusic
2. marc:version -> version

The first rule applies only if the work type (in bib records) is any of MovingImage, Music, Audio, NotatedMusic.

See https://jira.kb.se/browse/LXL-1556 for more info.
*/

import whelk.datatool.DocumentItem

changed = getReportWriter("changed.tsv")
conflict = getReportWriter("conflict.tsv")
likelyCorrectVersion = getReportWriter("likely-correct-version.txt")

VERSION = 'version'
MARC_ARRANGED = 'marc:arrangedStatementForMusic'
MARC_VERSION = 'marc:version'

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

MARC_VERSION_BIB_PATHS =
        [
                ['instanceOf'],
                ['instanceOf', 'expressionOf'],
                ['instanceOf', 'hasPart'],
                ['instanceOf', 'relationship', '*', 'entity'],
                ['instanceOf', 'subject']
        ]

MARC_VERSION_AUTH_PATHS =
        [
                [],
                ['hasVariant'],
                ['relatedTo'],
                ['subject'],
                ['marc:hasEstablishedHeadingLinkingEntryUniformTitle']
        ]

OK_WORK_TYPES = ['MovingImage', 'Music', 'Audio', 'NotatedMusic']

selectByCollection('bib') { DocumentItem docItem ->
    if (workType(docItem) in OK_WORK_TYPES) {
        oldToNew(docItem, VERSION_BIB_PATHS, VERSION, MARC_ARRANGED)
    }
    oldToNew(docItem, MARC_VERSION_BIB_PATHS, MARC_VERSION, VERSION)
}

selectByCollection('auth') { DocumentItem docItem ->
    oldToNew(docItem, VERSION_AUTH_PATHS, VERSION, MARC_ARRANGED)
    oldToNew(docItem, MARC_VERSION_AUTH_PATHS, MARC_VERSION, VERSION)
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
/**
 * Move hasTitle from instanceOf.expressionOf to instanceOf.
 * Also move a few other properties that are associated with the title.
 * Remove expressionOf altogether given that the move is successful (i.e. no conflicts/deviations).
 * The procedure is the same for both linked and local entities associated with expressionOf.
 *
 * Normalize titles according to given curated spreadsheets that map expressionOf entities to desired forms.
 * Local expressionOf entities are identified by a string representation. Linked expression are identified by their URI's.
 */

import whelk.util.Unicode
import whelk.filter.LanguageLinker

moved = getReportWriter('moved.tsv')
propertyAlreadyExists = getReportWriter('property-already-exists.tsv')
langDiff = getReportWriter('lang-diff.tsv')
hasFieldRef = getReportWriter('has-fieldref.txt')

HAS_TITLE = 'hasTitle'
MAIN_TITLE = 'mainTitle'
INSTANCE_OF = 'instanceOf'
LANGUAGE = 'language'
TRANSLATION_OF = 'translationOf'
EXPRESSION_OF = 'expressionOf'
ID = '@id'
TYPE = '@type'

langLinker = getLangLinker()
languageNames = langLinker.map.keySet() + langLinker.substitutions.keySet() + langLinker.ambiguousIdentifiers.keySet()

localExpressionOfToPrefTitle = loadLocalExpressionOfToPrefTitleMappings('title-mappings/local-expressionOf.tsv')
linkedExpressionOfToPrefTitle = loadLinkedExpressionOfToPrefTitleMappings('title-mappings/linked-expressionOf.tsv')
bibleToVersion = loadBibleVersions('title-mappings/bible-versions.tsv')

TITLE_RELATED_PROPS = ['musicMedium', 'version', 'legalDate', 'originDate', 'marc:arrangedStatementForMusic']

def where = """
    collection = 'bib'
        and data #>> '{@graph, 1, instanceOf, expressionOf}' is not null 
"""

selectBySqlWhere(where) {
    def id = it.doc.shortId
    def instance = it.graph[1]
    def work = instance[INSTANCE_OF]

    def isMusic = work[TYPE] in ['Music', 'NotatedMusic']

    if (asList(work[EXPRESSION_OF]).size() != 1) {
        return
    }

    def expressionOf = asList(work[EXPRESSION_OF])[0]

    def normalized = false

    if (expressionOf[ID]) {
        def prefTitle = linkedExpressionOfToPrefTitle[expressionOf[ID]]
        if (prefTitle) {
            def uniformWorkTitle = loadThing(expressionOf[ID])
            expressionOf = uniformWorkTitle
            // Take preferred title from given list
            expressionOf[HAS_TITLE] =
                    [
                            [
                                    (TYPE)      : 'Title',
                                    (MAIN_TITLE): prefTitle,
                                    'source'    : [[(ID): expressionOf[ID]]]
                            ]
                    ]
            normalized = true
        } else {
            incrementStats('Unhandled uniform work titles', expressionOf[ID])
            return
        }
    } else {
        if (expressionOf['marc:fieldref']) {
            hasFieldRef.println(id)
            return
        }
        def expressionOfAsString = stringify(expressionOf)
        def prefTitle = localExpressionOfToPrefTitle[expressionOfAsString]
        if (prefTitle) {
            if (bibleToVersion[expressionOfAsString]) {
                expressionOf['version'] = bibleToVersion[expressionOfAsString]
            }
            expressionOf[HAS_TITLE] =
                    [
                            [
                                    (TYPE)      : 'Title',
                                    (MAIN_TITLE): prefTitle,
                                    'source'    : [[(ID): 'https://id.kb.se/term/uniformWorkTitle']]
                            ]
                    ]
            normalized = true
        }
    }

    moveLanguagesFromTitle(expressionOf)
    langLinker.linkAll(work)
    def workLang = asList(work[LANGUAGE])
    def trlOf = asList(work[TRANSLATION_OF])[0]
    def trlOfLang = trlOf ? asList(trlOf[LANGUAGE]) : []
    langLinker.linkLanguages(expressionOf, workLang + trlOfLang)
    def exprOfLang = asList(expressionOf[LANGUAGE])

    if (!(workLang + trlOfLang).containsAll(exprOfLang)) {
        langDiff.println([id, expressionOf[LANGUAGE], workLang + trlOfLang].join('\t'))
        return
    }

    if (expressionOf[HAS_TITLE]) {
        if (!normalized && instance['issuanceType'] == 'Monograph') {
            incrementStats('Title not normalized (Monographs)', stringify(expressionOf))
        }
        List moveThese = TITLE_RELATED_PROPS + HAS_TITLE
        if (isMusic) {
            moveThese.add('musicKey')
        }
        if (tryCopyToTarget(expressionOf, work, id, moveThese)) {
            work.remove(EXPRESSION_OF)
            it.scheduleSave()
        }
    }
}

boolean tryCopyToTarget(Map from, Map target, String id, Collection properties = null) {
    def copyThese = properties ? from.keySet().intersect(properties) : from.keySet()

    def conflictingProps = copyThese.intersect(target.keySet())
    if (conflictingProps && conflictingProps.any { from[it] != target[it] }) {
        propertyAlreadyExists.println([id, conflictingProps].join('\t'))
        return false
    }

    if (copyThese) {
        from.each { k, v ->
            if (k in copyThese) {
                target[k] = v
            }
        }
        moved.println([id, copyThese, target.subMap(copyThese)].join('\t'))
    }

    return true
}

// mainTitle: "Haggada. Jiddisch & Hebreiska" --> mainTitle: Haggada, language: [[@id:https://id.kb.se/language/yid], [@id:https://id.kb.se/language/heb]]
void moveLanguagesFromTitle(Map expressionOf) {
    def languages = []

    asList(expressionOf[HAS_TITLE]).each { t ->
        if (!t[MAIN_TITLE]) {
            return
        }
        def (mt, l) = splitTitleLanguages(t[MAIN_TITLE])
        if (l) {
            languages += l
            t[MAIN_TITLE] = mt
        }
    }

    if (languages) {
        expressionOf[LANGUAGE] = asList(expressionOf[LANGUAGE]) + languages.collect { [(TYPE): 'Language', 'label': it] }
    }
}

Tuple2<String, List<String>> splitTitleLanguages(String mainTitle) {
    List languages = []
    String title = mainTitle
    (mainTitle =~ /^(?<title>.*)\.(?<languages>[^.]+)\.?\s*$/).with {
        if (matches()) {
            List l = group('languages')
                    .split(",| & | och | and ")
                    .collect { Unicode.trimNoise(it.toLowerCase()) }

            if (l && l.every { it in languageNames }) {
                languages.addAll(l)
                title = group('title')
            }
        }
    }

    return new Tuple2(title, languages)
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

// Represent a local work entity as a string, e.g. "Bible. · [O.T., Psalms., Sternhold and Hopkins.] · eng"
String stringify(Map work) {
    return [stringifyTitle(work), stringifyProps(work, TITLE_RELATED_PROPS)].grep().join(' · ').trim()
}

String stringifyProps(Map work, List props) {
    def values = props.collect { getAtPath(work, asList(it)) }
    def langcode = asList(work[LANGUAGE]).collect { (it[ID] ?: asList((it['label'] ?: '')).first()).split('/').last() }
    return (values + langcode).grep().join(' · ')
}

String stringifyTitle(Map work) {
    def titleKeys = ['mainTitle', 'partName', 'partNumber', 'marc:formSubheading']
    def paths = titleKeys.collect { [HAS_TITLE, 0] + it }
    def titleParts = paths.collect {
        getAtPath(work, it)
            ?: getAtPath(work, it.dropRight(1) + [it.last() + 'ByLang'])?.find { it.key.contains('-t-') }?.value
    }.grep()

    return titleParts.join(' · ')
}

// e.g. {"Bible. · [O.T., Psalms., Sternhold and Hopkins.] · eng": "Bibeln. Psaltaren"}
Map loadLocalExpressionOfToPrefTitleMappings(String filename) {
    return new File(scriptDir, filename).readLines().drop(1).collectEntries {
        def (prefTitle, stringifiedExpressionOf) = it.split('\t')
        [stringifiedExpressionOf, prefTitle]
    }
}

// e.g. {"https://libris.kb.se/0xbddxzj09vsjl9#it": "Bibeln. Haggai"}
Map loadLinkedExpressionOfToPrefTitleMappings(String filename) {
    return new File(scriptDir, filename).readLines().drop(1).collectEntries {
        def (uniformWorkTitleIri, prefTitle) = it.split('\t')
        // replace only needed in test environments
        [uniformWorkTitleIri.replace("https://libris.kb.se/", baseUri.toString()), prefTitle]
    }
}

// e.g. {"Bible. · [N.T., Gospels., Campbell.] · eng": "Campbell"}
Map loadBibleVersions(String filename) {
    return new File(scriptDir, filename).readLines().drop(1).collectEntries {
        def (bible, version) = it.split('\t')
        [bible, version]
    }
}

LanguageLinker getLangLinker() {
    LanguageLinker linker

    selectByIds(['https://id.kb.se/vocab/']) {
        linker = it.whelk.normalizer.normalizers.find { it.getNormalizer() instanceof LanguageLinker }.getNormalizer()
    }

    return linker
}
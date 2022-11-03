import whelk.util.Unicode
import whelk.filter.LanguageLinker
import static whelk.JsonLd.looksLikeIri

moved = getReportWriter('moved.tsv')
propertyAlreadyExists = getReportWriter('property-already-exists.tsv')
brokenLinks = getReportWriter('broken-links.tsv')
langDiff = getReportWriter('lang-diff.tsv')
linked = getReportWriter('linked.tsv')

langLinker = getLangLinker()
languageNames = langLinker.map.keySet() + langLinker.substitutions.keySet() + langLinker.ambiguousIdentifiers.keySet()

HAS_TITLE = 'hasTitle'
MAIN_TITLE = 'mainTitle'
INSTANCE_OF = 'instanceOf'
LANGUAGE = 'language'
TRANSLATION_OF = 'translationOf'
EXPRESSION_OF = 'expressionOf'
ID = '@id'
TYPE = '@type'

prefTitleToWorkHub = queryDocs(['@type': ['WorkHub']]).collectEntries { [it[HAS_TITLE][0][MAIN_TITLE], it[ID]] }
localExpressionOfToPrefTitle = getLocalExpressionOfMappings('hubs.tsv')

TITLE_RELATED_PROPS = ['hasTitle', 'musicKey', 'musicMedium', 'version', 'marc:version', 'marc:fieldref', 'legalDate', 'originDate']

def where = """
    collection = 'bib'
        and data['@graph'][1]['instanceOf']['expressionOf'] is not null 
"""

selectBySqlWhere(where) {
    def id = it.doc.shortId
    def work = it.graph[1][INSTANCE_OF]

    def target = work[TYPE] in ['Music', 'NotatedMusic']
            ? work
            : (work[TRANSLATION_OF] ?: work)
    def targetProperty = target == work ? INSTANCE_OF : TRANSLATION_OF

    if (asList(work[EXPRESSION_OF]).size() != 1 || asList(target).size() != 1) {
        return
    }

    def expressionOf = asList(work[EXPRESSION_OF])[0]
    target = asList(target)[0]

    if (expressionOf[ID]) {
        def hub = loadThing(expressionOf[ID])
        if (!hub) {
            brokenLinks.println([id, expressionOf[ID]].join('\t'))
            return
        }
        // Already a WorkHub hub, keep as is
        else if (hub[TYPE] == 'Hub') {
            hubLinks.println([id, expressionOf[ID], expressionOf[ID]].join('\t')) // id   old link    new link
        }
        // Uniform work title has a replacement hub. Relink to the better hub (WorkHub).
        else (hub[EXPRESSION_OF]?[ID]) {
            hubLinks.println([id, expressionOf[ID], hub[EXPRESSION_OF][ID]].join('\t'))
            expressionOf[ID] = hub[EXPRESSION_OF][ID]
            it.scheduleSave()
        }

        // Shouldn't reach here if all linked uniform work title have been taken care of
        return
    }

    linkLangs(expressionOf, target)
    if (!compatibleLangs(expressionOf, target)) {
        langDiff.println([id, targetProperty, expressionOf[LANGUAGE], target[LANGUAGE]].join('\t'))
        return
    }

    // Try match existing good hub on title
    def newHub = findHub(expressionOf)
    if (newHub) {
        expressionOf.clear()
        expressionOf[ID] = newHub
        it.scheduleSave()
        return
    }

    if (tryCopyTitle(expressionOf, target, targetProperty, id)) {
        work.remove(EXPRESSION_OF)
        it.scheduleSave()
    }
}

boolean tryCopyTitle(Map expressionOf, Map target, String targetProperty, String id) {
    //TODO: Not sure that all properties should move to target when targetProperty = translationOf, needs further analysis
    def copyThese = expressionOf.keySet().intersect(TITLE_RELATED_PROPS)
    if (!copyThese.contains(HAS_TITLE)) {
        return false
    }

    def conflictingProps = copyThese.intersect(target.keySet())
    if (conflictingProps) {
        propertyAlreadyExists.println([id, targetProperty, conflictingProps].join('\t'))
        return false
    }

    expressionOf.each { k, v ->
        if (k in copyThese) {
            target[k] = v
        }
    }
//    normalizePunctuation(target[HAS_TITLE])
    moved.println([id, targetProperty, copyThese, target.subMap(TITLE_RELATED_PROPS + LANGUAGE)].join('\t'))
    return true
}

void linkLangs(Map expressionOf, Map target) {
    moveLanguagesFromTitle(expressionOf, expressionOf[HAS_TITLE])
    langLinker.linkLanguages(target)
    langLinker.linkLanguages(expressionOf, asList(target[LANGUAGE]))
}

boolean compatibleLangs(Map expressionOf, Map target) {
    asList(target[LANGUAGE]).containsAll(expressionOf[LANGUAGE])
}

void normalizePunctuation(Object title) {
    //TODO
}

void moveLanguagesFromTitle(Map expressionOf) {
    def languages = []

    asList(expressionOf[HAS_TITLE]).each { t ->
        if (t[MAIN_TITLE] instanceof String) {
            def (mt, l) = splitTitleLanguages(t[MAIN_TITLE])
            if (l) {
                languages += l
                t[MAIN_TITLE] = mt
            }
        }
        if (t[MAIN_TITLE] instanceof List) {
            t[MAIN_TITLE] = t[MAIN_TITLE].collect {
                def (mt, l) = splitTitleLanguages(it)
                if (l) {
                    languages += l
                    return mt
                }
                return it
            }
        }
    }

    if (languages) {
        expressionOf[LANGUAGE] = asList(expressionOf[LANGUAGE]) + languages
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

// For matching local expressionOf with a WorkHub
String findHub(Map expressionOf) {
    def expressionOfAsString = stringify(expressionOf)
    def prefTitle = localExpressionOfToPrefTitle[expressionOfAsString]
    if (prefTitle) {
        def matchedHub = prefTitleToWorkHub[prefTitle]
        if (matchedHub) {
            linked.println([expressionOfAsString, prefTitle, matchedHub].join('\t'))
            incrementStats("$prefTitle · $matchedHub", expressionOfAsString)
            return matchedHub
        }
    }
}

// Represent a local work entity as a string, e.g. "Bible. · [O.T., Psalms., Sternhold and Hopkins.] · eng"
String stringify(Map work) {
    def titleKeys = ['mainTitle', 'partName', 'partNumber', 'marc:formSubheading']
    def keys = (titleKeys.collect { ['hasTitle', 0] + it } + TITLE_RELATED_PROPS - HAS_TITLE)
    def props = keys.collect { getAtPath(work, asList(it)) }
    def langcode = asList(work[LANGUAGE]).collect { (it[ID] ?: asList((it['label'] ?: '')).first()).split('/').last() }
    return (props + langcode).grep().join(' · ')
}

LanguageLinker getLangLinker() {
    //TODO: Make LanguageLinker accessible from whelk
}

// e.g. {"Bible. · [O.T., Psalms., Sternhold and Hopkins.] · eng": "Bibeln. Psaltaren"}
Map getLocalExpressionOfMappings(String filename) {
    def localExpressionOfToPrefTitle = [:]

    new File(scriptDir, filename).splitEachLine('\t') { row ->
        if (row.size() == 5) {
            def localOrUniform = row[0]
            def (prefTitle, localExpressionOf) = row[3..4]
            if (!looksLikeIri(localOrUniform) && prefTitle) {
                localExpressionOfToPrefTitle[localExpressionOf] = prefTitle
            }
        }
    }

    return localExpressionOfToPrefTitle
}
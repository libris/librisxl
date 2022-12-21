import whelk.util.Unicode
import whelk.util.Statistics
import whelk.filter.LanguageLinker
import static whelk.util.Unicode.normalize

moved = getReportWriter('moved.tsv')
propertyAlreadyExists = getReportWriter('property-already-exists.tsv')
brokenLinks = getReportWriter('broken-links.tsv')
langDiff = getReportWriter('lang-diff.tsv')
relinked = getReportWriter('relinked.tsv')
linked = getReportWriter('linked-mappings-from-file.tsv')

linkedStats = new StatsReport(getReportWriter('stats-linked-mappings-from-file.txt'), 3)
notLinkedStats = new StatsReport(getReportWriter('stats-not-linked.txt'), 3)
notLinkedStatsTranslations = new StatsReport(getReportWriter('stats-not-linked-translations.txt'), 3)
notLinkedStatsOrigLang = new StatsReport(getReportWriter('stats-not-linked-orig-lang.txt'), 3)
instanceTitles = new StatsReport(getReportWriter('stats-instance-titles.txt'), 3)
instanceTitlesTranslations = new StatsReport(getReportWriter('stats-instance-titles-translations.txt'), 3)
unhandledUniformWorkTitles = new StatsReport(getReportWriter('unhandled-uniform-work-titles.txt'), 3)

HAS_TITLE = 'hasTitle'
MAIN_TITLE = 'mainTitle'
INSTANCE_OF = 'instanceOf'
LANGUAGE = 'language'
TRANSLATION_OF = 'translationOf'
EXPRESSION_OF = 'expressionOf'
ID = '@id'
TYPE = '@type'
WORK_HUB = 'WorkHub'

langLinker = getLangLinker()
languageNames = langLinker.map.keySet() + langLinker.substitutions.keySet() + langLinker.ambiguousIdentifiers.keySet()

hubTitleToWorkHub = loadHubTitleToHubMappings()
localExpressionOfToHubTitle = loadLocalExpressionOfToHubTitleMappings('hub-data/local-expressionOf.tsv')
hymnsAndBibles = loadHymnsAndBibles('hub-data/psalmböcker-och-biblar.tsv')

TITLE_RELATED_PROPS = ['musicKey', 'musicMedium', 'version', 'marc:version', 'marc:fieldref', 'legalDate', 'originDate']

def where = """
    collection = 'bib'
        and data['@graph'][1]['instanceOf']['expressionOf'] is not null 
"""

selectBySqlWhere(where) {
    def id = it.doc.shortId
    def instance = it.graph[1]
    def work = instance[INSTANCE_OF]

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
        else if (hub[TYPE] == WORK_HUB) {
            return
        }
        // Uniform work title has a replacement hub. Relink to the better hub (WorkHub).
        else if (hub[EXPRESSION_OF]) {
            relinked.println([id, hub[ID], hub[EXPRESSION_OF][ID]].join('\t')) // id   old link    new link
            expressionOf[ID] = hub[EXPRESSION_OF][ID]
            it.scheduleSave()
        }

        // Shouldn't reach here if all linked uniform work titles have been taken care of
        unhandledUniformWorkTitles.s.increment('Unhandled uniform work titles', hub[ID], id)
        return
    }

    // Try match existing good hub on title
    def newHub = findHub(expressionOf, id)
    if (newHub) {
        def stringified = stringify(expressionOf)
        if (hymnsAndBibles[stringified] && !tryCopyToTarget(hymnsAndBibles[stringified], work, INSTANCE_OF, id)) {
            return
        }
        if (tryCopyToTarget(expressionOf, work, INSTANCE_OF, id, TITLE_RELATED_PROPS)) {
            expressionOf.clear()
            expressionOf[ID] = newHub
            it.scheduleSave()
        }
        return
    }

    if (instance.issuanceType == 'Monograph') {
        def stringifiedTitle = stringifyTitle(expressionOf)
        def stringified = stringify(expressionOf)
        def stringifiedInstanceTitle = stringifyInstanceTitle(asList(instance[HAS_TITLE])[0])
        notLinkedStats.s.increment('Not linked expressionOf (monograph)', stringified, id)
        if (targetProperty == TRANSLATION_OF) {
            notLinkedStatsTranslations.s.increment('expressionOf to translationOf (monographs)', stringified, id)
            instanceTitlesTranslations.s.increment(stringifiedTitle, stringifiedInstanceTitle, id)
        } else {
            notLinkedStatsOrigLang.s.increment('expressionOf to instanceOf (monographs)', stringified, id)
            instanceTitles.s.increment(stringifiedTitle, stringifiedInstanceTitle, id)
        }
    }

    moveLanguagesFromTitle(expressionOf)
    linkLangs(expressionOf, work, targetProperty == TRANSLATION_OF ? target : [:])

    if (!compatibleLangs(expressionOf, work, target)) {
        def cols = [id, targetProperty, expressionOf[LANGUAGE], work[LANGUAGE]]
        if (targetProperty == TRANSLATION_OF) {
            cols.add(target[LANGUAGE])
        }
        langDiff.println(cols.join('\t'))
        return
    }

    if (expressionOf[HAS_TITLE] && tryCopyToTarget(expressionOf, target, targetProperty, id, TITLE_RELATED_PROPS + HAS_TITLE)) {
        work.remove(EXPRESSION_OF)
        it.scheduleSave()
    }
}

[
        linkedStats,
        notLinkedStats,
        notLinkedStatsTranslations,
        notLinkedStatsOrigLang,
        instanceTitles,
        instanceTitlesTranslations,
        unhandledUniformWorkTitles
].each {
    it.print()
}

boolean tryCopyToTarget(Map from, Map target, String targetProperty, String id, Collection properties = null) {
    //TODO: Not sure that all properties should move to target when targetProperty = translationOf, needs further analysis
    def copyThese = properties ? from.keySet().intersect(properties) : from.keySet()

    def conflictingProps = copyThese.intersect(target.keySet())
    if (conflictingProps && conflictingProps.any { from[it] != target[it] }) {
        propertyAlreadyExists.println([id, targetProperty, conflictingProps].join('\t'))
        return false
    }

    if (copyThese) {
        from.each { k, v ->
            if (k in copyThese) {
                target[k] = v
            }
        }
//        normalizePunctuation(target[HAS_TITLE])
        moved.println([id, targetProperty, copyThese, target.subMap(copyThese + LANGUAGE)].join('\t'))
    }

    return true
}

void linkLangs(Map expressionOf, Map work, Map translationOf) {
    def workLang = work.subMap(LANGUAGE)
    langLinker.linkLanguages(workLang)
    work[LANGUAGE] = workLang[LANGUAGE]
    langLinker.linkLanguages(translationOf)
    langLinker.linkLanguages(expressionOf, asList(work[LANGUAGE]) + asList(translationOf[LANGUAGE]))
}

boolean compatibleLangs(Map expressionOf, Map work, Map target) {
    (asList(work[LANGUAGE]) + asList(target[LANGUAGE])).containsAll(asList(expressionOf[LANGUAGE]))
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

// For matching local expressionOf with a WorkHub
String findHub(Map expressionOf, String id) {
    def expressionOfAsString = stringify(expressionOf)
    def hubTitle = localExpressionOfToHubTitle[expressionOfAsString]
    if (hubTitle) {
        hubTitle = normalize(hubTitle)
    }

    def matchedHub = hubTitleToWorkHub[hubTitle]
    if (matchedHub) {
        linked.println([id, expressionOfAsString, hubTitle, matchedHub].join('\t'))
        linkedStats.s.increment("$hubTitle · $matchedHub", expressionOfAsString, id)
        return matchedHub
    }
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
    def titleParts = paths.collect { getAtPath(work, asList(it)) }.grep()

    return titleParts.join(' · ')
}

String stringifyInstanceTitle(Object o) {
    def titleParts = ['mainTitle', 'titleRemainder', 'subtitle', 'hasPart', 'partNumber', 'partName', 'marc:parallelTitle', 'marc:equalTitle']

    if (o instanceof String) {
        return o
    }
    if (o instanceof List) {
        return o
                .collect { stringifyInstanceTitle(it) }
                .join(' || ')
    }
    if (o instanceof Map) {
        return titleParts
                .findResults { ((Map) o).get(it) }
                .collect { stringifyInstanceTitle(it) }
                .join(' · ')
    }

    throw new RuntimeException(String.format("unexpected type: %s for %s", o.class.getName(), o))
}

Map loadHubTitleToHubMappings() {
    return queryDocs([(TYPE): [WORK_HUB]]).collectEntries { [it[HAS_TITLE][0][MAIN_TITLE], it[ID]] }
}

// e.g. {"Bible. · [O.T., Psalms., Sternhold and Hopkins.] · eng": "Bibeln. Psaltaren"}
Map loadLocalExpressionOfToHubTitleMappings(String filename) {
    return new File(scriptDir, filename).readLines().drop(1).collectEntries {
        def (hubTitle, stringifiedExpressionOf) = it.split('\t')
        [stringifiedExpressionOf, hubTitle]
    }
}

Map loadHymnsAndBibles(String filename) {
    def rows = new File(scriptDir, filename).readLines()
    def propertyNames = rows.pop().split('\t')
    return new File(scriptDir, filename).readLines().drop(1).collectEntries {
        def values = it.split('\t', -1) as List
        def stringifiedExpressionOf = values.pop()
        def data = [propertyNames.drop(1), values].transpose()
                .collectEntries { it }
                .findAll { it.value }
        [stringifiedExpressionOf, data]
    }
}

LanguageLinker getLangLinker() {
    LanguageLinker linker

    selectByIds(['https://id.kb.se/vocab/']) {
        linker = it.whelk.normalizer.normalizers.find { it.getLinker() instanceof LanguageLinker }.getLinker()
    }

    return linker
}

class StatsReport {
    PrintWriter report
    Statistics s

    StatsReport(PrintWriter report, int numExamples) {
        this.report = report
        this.s = new Statistics(numExamples)
    }

    void print() {
        report.withCloseable {
            s.print(0, it)
        }
    }

    void filterCategories(int min) {
        s.c.removeAll { k, v ->
            v.values().collect { it.intValue() }.sum() < min
        }
    }
}
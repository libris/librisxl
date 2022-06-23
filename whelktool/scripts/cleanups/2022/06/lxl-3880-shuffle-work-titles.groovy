import whelk.filter.LanguageLinker
import whelk.util.Statistics
import whelk.util.Unicode

import java.util.concurrent.ConcurrentLinkedQueue

def moved = getReportWriter('moved.tsv')
def blankLangRemoved = getReportWriter('blank-lang-removed.tsv')
def extraLang = getReportWriter('lang-added.tsv')
def propertyAlreadyExists = getReportWriter('property-already-exists.tsv')
def brokenLinks = getReportWriter('broken-links.tsv')

languageLinker = buildLanguageMap()
Map a = (Map<String, List>) languageLinker.ambiguousIdentifiers
ambiguousLangNames = a.keySet()
ambiguousLangIds = a.values().flatten()

languageNames = languageLinker.map.keySet() + languageLinker.substitutions.keySet() + languageLinker.ambiguousIdentifiers.keySet()

INSTANCE_OF = "instanceOf"
HAS_TITLE = "hasTitle"
EXPRESSION_OF = "expressionOf"
TRANSLATION_OF = "translationOf"
LANGUAGE = "language"
ID = '@id'

selectByCollection('bib') { bib ->
    Map instance = bib.graph[1]
    def shortId = bib.doc.shortId
    def work = instance[INSTANCE_OF]

    if (!work || work instanceof List)
        return

    def workTitle = work[HAS_TITLE]
    def expressionOf = asList(work[EXPRESSION_OF])[0]
    def translationOf = asList(work[TRANSLATION_OF])[0]

    def worklang = asList(work[LANGUAGE])
    def whichOne = worklang.findAll { it[ID] in ambiguousLangIds }
    // Remove superfluous blank languages
    def blankLang = worklang.find { !it[ID] }
    if (blankLang) {
        def mapped = mapBlankLanguages([blankLang], whichOne)
        if ((worklang - blankLang).toSet() == mapped.toSet()) {
            worklang.remove(blankLang)
            blankLangRemoved.println([shortId, blankLang.label, worklang].join('\t'))
        }
    }

    if (!expressionOf) {
        // Move title from instanceOf to translationOf
        if (translationOf && workTitle) {
            if (translationOf[HAS_TITLE] && translationOf[HAS_TITLE] != work[HAS_TITLE]) {
                propertyAlreadyExists.println([shortId, "$INSTANCE_OF -> $TRANSLATION_OF", HAS_TITLE, work[HAS_TITLE], translationOf[HAS_TITLE]].join('\t'))
            } else {
                translationOf[HAS_TITLE] = normalizeTitle(work[HAS_TITLE])
                moved.println([shortId, "$INSTANCE_OF -> $TRANSLATION_OF", HAS_TITLE, translationOf[HAS_TITLE], translationOf[LANGUAGE]].join('\t'))
                work.remove(HAS_TITLE)
                bib.scheduleSave()
            }
        }
        return
    }

    // Move expressionOf content primarily to translationOf, otherwise to instanceOf
    def (target, targetKey) = translationOf ? [translationOf, TRANSLATION_OF] : [work, INSTANCE_OF]

    // Linked uniform work title in expressionOf, move hasTitle to instanceOf and then remove the link
    if (expressionOf[ID]) {
        def linked = loadThing(expressionOf[ID])
        if (!linked) {
            brokenLinks.println([shortId, expressionOf[ID]].join('\t'))
            return
        }
        if (target[HAS_TITLE] && target[HAS_TITLE] != linked[HAS_TITLE]) {
            propertyAlreadyExists.println([shortId, "$EXPRESSION_OF -> $targetKey", HAS_TITLE, linked[HAS_TITLE], target[HAS_TITLE]].join('\t'))
        } else {
            target[HAS_TITLE] = normalizeTitle(linked[HAS_TITLE])
            moved.println([shortId, "$EXPRESSION_OF -> $targetKey", HAS_TITLE, target[HAS_TITLE], target[LANGUAGE]].join('\t'))
            work.remove(EXPRESSION_OF)
            bib.scheduleSave()
        }
        return
    }

    moveLanguagesFromTitle(expressionOf, whichOne)

    // Move the complement expressionOf.language \ instanceOf.language to instanceOf.language
    def langsAdded = []
    asList(expressionOf[LANGUAGE]).each { l ->
        asList(l.'@id' ? l : mapBlankLanguages([l], whichOne)).each {
            if (!(it in worklang)) {
                worklang << it
                langsAdded << it
            }
        }
    }
    if (langsAdded) {
        work[LANGUAGE] = worklang
        extraLang.println([shortId, langsAdded, worklang].join('\t'))
    }
    expressionOf.remove(LANGUAGE)

    // Move title from expressionOf
    if (expressionOf[HAS_TITLE]) {
        if (target[HAS_TITLE] && target[HAS_TITLE] != expressionOf[HAS_TITLE]) {
            propertyAlreadyExists.println([shortId, "$EXPRESSION_OF -> $targetKey", HAS_TITLE, expressionOf[HAS_TITLE], target[HAS_TITLE]].join('\t'))
        } else {
            target[HAS_TITLE] = normalizeTitle(expressionOf[HAS_TITLE])
            moved.println([shortId, "$EXPRESSION_OF -> $targetKey", HAS_TITLE, target[HAS_TITLE]].join('\t'))
            expressionOf.remove(HAS_TITLE)
        }
    }

    // Move remaining properties (except @type) to the same place as the title
    expressionOf.removeAll { k, v ->
        if (k == '@type')
            return true
        if (target[k] && target[k] != v) {
            propertyAlreadyExists.println([shortId, "$EXPRESSION_OF -> $targetKey", k, v, target[k]].join('\t'))
            return false
        } else {
            target[k] = v
            moved.println([shortId, "$EXPRESSION_OF -> $targetKey", k, target[k]].join('\t'))
            return true
        }
    }

    // No properties should remain unless there has been a conflict somewhere (reported in property-already-exists.tsv)
    if (expressionOf.isEmpty()) {
        work.remove(EXPRESSION_OF)
    }

    bib.scheduleSave()
}

// Remove single trailing period from mainTitle
def normalizeTitle(title) {
    def t = asList(title).collect().first()
    if (t.mainTitle)
        t.mainTitle = asList(t.mainTitle).first().replaceFirst(~/\s*(?<!\.)\.\s*$/, '')
    return title
}

// The following methods are copied (with minor additions) from 2021/04/lxl-3547-link-expressionOf.groovy

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

// Handle e.g. { "@type": "Language", "label": ["English & Tamil."] }
private List mapBlankLanguages(List languages, List whichLanguageVersion = []) {
    if (languages.size() == 1 && languages[0].label) {
        String label = asList(languages[0].label).first().toLowerCase()
        // to be able to map e.g. "Greek" to modern or classical Greek (LanguageLinker will use an existing linked sibling to decide when ambiguous)
        List copy = new ArrayList(
                ambiguousLangNames.any { label.contains(it) }
                        ? languages + whichLanguageVersion
                        : languages
        )
        Map m = ['l': copy]
        languageLinker.linkLanguages(m, 'l')
        return m['l']
    }

    return languages
}

// mainTitle: "Haggada. Jiddisch & Hebreiska" --> mainTitle: Haggada, language: [[@id:https://id.kb.se/language/yid], [@id:https://id.kb.se/language/heb]]
boolean moveLanguagesFromTitle(Map work, List whichLanguageVersion = []) {
    def (title, languages) = splitTitleLanguages(asList(work['hasTitle'])[0]?.mainTitle)

    if (!languages) {
        return false
    }

    def l = languages.collect { ['@type': 'Language', 'label': it] }

    if (work.language) {
        l += work.language
    }

    if (languages.any { it in ambiguousLangNames }) {
        l += whichLanguageVersion
    }

    Map m = ['l': l]
    languageLinker.linkLanguages(m, 'l')
    l = m.l

    l.each {
        if (it.label) {
            it.label = it.label.capitalize()
        }
    }

    asList(work['hasTitle']).first().mainTitle = title
    work.language = l

    return true
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

class AndLanguageLinker extends LanguageLinker {
    AndLanguageLinker(List ignoreCodes = [], Statistics stats = null) {
        super(ignoreCodes, stats)
    }

    @Override
    protected List split(labelOrCode) {
        if (labelOrCode instanceof List) {
            return labelOrCode
        }

        if (labelOrCode ==~ /^(.*,)*.*( & | och | and ).*/) {
            return labelOrCode.split(",| & | och | and ") as List
        }
    }
}

// from 2019/10/lxl-2737-remove-redundant-blank-languages
Map substitutions() {
    [
            'catalán'                         : 'katalanska',
            'dansk'                           : 'danska',
            'engl'                            : 'engelska',
            'fornkyrkoslaviska'               : 'fornkyrkslaviska',
            'francais'                        : 'franska',
            'inglés'                          : 'engelska',
            'jap'                             : 'jpn',
            'kroat'                           : 'kroatiska',
            'latviešu val'                    : 'lettiska',
            'latviešu valodā'                 : 'lettiska',
            'mongoliska språket'              : 'mongoliska språk',
            'mongoliska'                      : 'mongoliska språk',
            'mongoliskt språk'                : 'mongoliska språk',
            'ruotsi'                          : 'svenska',
            'schwed'                          : 'svenska',
            'suomi'                           : 'finska',
            'svensk'                          : 'svenska',
            'tigriniska'                      : 'tigrinska',
            'tornedalsfinska'                 : 'meänkieli',
            'á íslensku'                      : 'isländska',
            'česky'                           : 'tjeckiska',

            'arabiska (judearabiska)'         : 'judearabiska',
            'engelska (fornengelska)'         : 'fornengelska',
            'engelska (medelengelska)'        : 'medelengelska',
            'franska (fornfranska)'           : 'fornfranska',
            'franska (medelfranska)'          : 'medelfranska',
            'french (middle french)'          : 'medelfranska',
            'grekiska (nygrekiska)'           : 'nygrekiska',
            'nederländska (medelnederländska)': 'medelnederländska',
            'norska (nynorsk)'                : 'nynorska',
            'norska (nynorska)'               : 'nynorska',
            'samiska (lulesamiska)'           : 'lulesamiska',
            'samiska (nordsamiska)'           : 'nordsamiska',
            'svenska (fornsvenska)'           : 'fornsvenska',
            'tyska (lågtyska)'                : 'lågtyska',
            'tyska (medelhögtyska)'           : 'medelhögtyska',
            'tyska (medellågtyska)'           : 'medellågtyska',

            // Added in lxl-3547
            'polyglott'                       : 'flera språk',
            'pehlevi'                         : 'pahlavi',
            'kanbodjanska (khmer)'            : 'kambodjanska',
            'english (middle english)'        : 'medelengelska',
            'english (anglo-saxon)'           : 'fornengelska',
            'judetyska'                       : 'jiddisch',
            'italiensk'                       : 'italienska',
            'german (middle high german)'     : 'medelhögtyska',
            'skotsk-gaeliska'                 : 'skotsk gäliska',
            'nubiska'                         : 'nubiska språk',

            // Added in lxl-3880
            'polyglot'                        : 'flera språk',
            'magyar'                          : 'ungerska',
            'español'                         : 'spanska',
            'holländska'                      : 'nederländska',
            '(pol.)'                          : 'polska',
            'dt'                              : 'tyska',
            'dt.'                             : 'tyska',
            'vertimas į lietuvių k'           : 'litauiska',
            'slovenski jezik'                 : 'slovenska'

            // Also seen
            // Fornnorska
            // Fornryska
            // Irish (Old Irish)
            // Fornspanska
            // Medellågtyska
            // Luchazi
            // Egyptian
            // anglo-norman
            // spanska (medelspanska)
            // vepsiska
            // gäliska
    ]
}

LanguageLinker buildLanguageMap() {
    def q = [
            "@type": ["Language"],
            "q"    : ["*"],
            '_sort': ["@id"]
    ]

    LanguageLinker linker = new AndLanguageLinker([], new Statistics().printOnShutdown())
    ConcurrentLinkedQueue<Map> languages = new ConcurrentLinkedQueue<>()
    selectByIds(queryIds(q).collect()) { languages.add(it.graph[1]) }
    languages.forEach({ l -> linker.addDefinition(l) })

    linker.addSubstitutions(substitutions())

    linker.addMapping('grekiska', 'https://id.kb.se/language/gre')
    linker.addMapping('grekiska', 'https://id.kb.se/language/grc')
    linker.addMapping('greek', 'https://id.kb.se/language/gre')
    linker.addMapping('greek', 'https://id.kb.se/language/grc')

    linker.addMapping('syriska', 'https://id.kb.se/language/syc')
    linker.addMapping('syriska', 'https://id.kb.se/language/syr')

    // Added in lxl-3880
    linker.addMapping('norsk', 'https://id.kb.se/language/nno')
    linker.addMapping('norsk', 'https://id.kb.se/language/nor')
    linker.addMapping('serbokroatiska', 'https://id.kb.se/language/hrv')
    linker.addMapping('serbokroatiska', 'https://id.kb.se/language/srp')

    return linker
}


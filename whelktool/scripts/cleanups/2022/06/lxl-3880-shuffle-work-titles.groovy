import whelk.filter.LanguageLinker
import whelk.util.Statistics
import whelk.util.Unicode

import java.util.concurrent.ConcurrentLinkedQueue

moved = getReportWriter('moved.tsv')
blankLangRemoved = getReportWriter('blank-lang-removed.tsv')
extraLang = getReportWriter('lang-added.tsv')
propertyAlreadyExists = getReportWriter('property-already-exists.tsv')
brokenLinks = getReportWriter('broken-links.tsv')
multiple = getReportWriter('multiple.txt')

languageLinker = buildLanguageMap()
Map a = (Map<String, List>) languageLinker.ambiguousIdentifiers
ambiguousLangNames = a.keySet()
ambiguousLangIds = a.values().flatten()

languageNames = languageLinker.map.keySet() + languageLinker.substitutions.keySet() + languageLinker.ambiguousIdentifiers.keySet()

INSTANCE_OF = "instanceOf"
HAS_PART = "hasPart"
HAS_TITLE = "hasTitle"
EXPRESSION_OF = "expressionOf"
TRANSLATION_OF = "translationOf"
LANGUAGE = "language"
ID = '@id'

selectByCollection('bib') { bib ->
    Map instance = bib.graph[1]
    def instanceOf = instance.find { it.key == INSTANCE_OF }
    def shortId = bib.doc.shortId

    if (!instanceOf)
        return

    if (multipleValues(instanceOf.value)) {
        multiple.println("$shortId\t$INSTANCE_OF")
        return
    }

    Map work = asList(instanceOf.value)[0]

    // Remove superfluous blank languages
    def worklang = asList(work[LANGUAGE])
    def whichOne = worklang.findAll { it[ID] in ambiguousLangIds }
    def blankLang = worklang.find { !it[ID] }
    if (blankLang) {
        def mapped = mapBlankLanguages([blankLang], whichOne)
        if ((worklang - blankLang).toSet() == mapped.toSet()) {
            worklang.remove(blankLang)
            blankLangRemoved.println([shortId, blankLang.label, worklang].join('\t'))
        }
    }

    def expressionOf = work.find { it.key == EXPRESSION_OF }

    if (expressionOf) {
        if (multipleValues(expressionOf.value)) {
            multiple.println("$shortId\t$EXPRESSION_OF")
            return
        }

        // Move expressionOf content primarily to translationOf, otherwise to instanceOf
        def target = work.find { it.key == TRANSLATION_OF } ?: instanceOf

        Map hub = asList(expressionOf.value)[0]
        if (hub[ID]) {
            expressionOf.value = loadThing(hub[ID])
            if (!expressionOf.value) {
                brokenLinks.println([shortId, hub[ID]].join('\t'))
            }
            // Linked uniform work title in expressionOf: move hasTitle to target and then remove the link
            else if (moveProperty(shortId, HAS_TITLE, expressionOf, target)) {
                work.remove(EXPRESSION_OF)
            }
        }
        // Move title from local expressionOf to target
        else if (moveProperty(shortId, HAS_TITLE, expressionOf, target)) {
            hub.remove(HAS_TITLE)
            // Move the complement expressionOf.language \ instanceOf.language to instanceOf.language
            if (hub[LANGUAGE]) {
                def langsAdded = []
                asList(hub[LANGUAGE]).each { l ->
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
                hub.remove(LANGUAGE)
            }
            // Move remaining properties (except @type) to the same place as the title
            hub.removeAll {
                it.key == '@type' ?: moveProperty(shortId, it.key, expressionOf, target)
            }
        }

        if (hub.isEmpty()) {
            work.remove(EXPRESSION_OF)
        }

        bib.scheduleSave()
    }
    // Move title from instanceOf to translationOf if translationOf exists
    else if (moveProperty(shortId, HAS_TITLE, instanceOf, work.find { it.key == TRANSLATION_OF })) {
        work.remove(HAS_TITLE)
        bib.scheduleSave()
    }

    // Move title from hasPart to hasPart.translationOf if hasPart.translationOf exists
    work[HAS_PART]?.each { Map p ->
        if (moveProperty(shortId, HAS_TITLE, Map.entry(HAS_PART, p), p.find { it.key == TRANSLATION_OF })) {
            p.remove(HAS_TITLE)
            bib.scheduleSave()
        }
    }
}

boolean moveProperty(String id, String propToMove, Map.Entry from, Map.Entry target) {
    def f = asList(from.value)[0]

    if (f[propToMove] && target) {
        def whatMove = "${from.key} -> ${target.key}"
        def t = asList(target.value)[0]

        if (multipleValues(t)) {
            multiple.println("$id\t$target.key")
            return false
        }

        if (t[propToMove]) {
            propertyAlreadyExists.println([id, whatMove, propToMove, f[propToMove], t[propToMove]].join('\t'))
        } else {
            def cols

            if (propToMove == HAS_TITLE) {
                def movedLang = moveLanguagesFromTitle(f, asList(t[LANGUAGE]).findAll { it[ID] in ambiguousLangIds })
                def normalized = normalizeTitle(f[HAS_TITLE])
                cols = [id, whatMove, HAS_TITLE, f[HAS_TITLE], (movedLang || normalized)]
                if (target.key == TRANSLATION_OF)
                    cols << t[LANGUAGE]
            } else {
                cols = [id, whatMove, propToMove, f[propToMove]]
            }

            t[propToMove] = f[propToMove]

            moved.println(cols.join('\t'))

            return true
        }
    }

    return false
}

// Remove single trailing period and trailing whitespace from mainTitle
boolean normalizeTitle(title) {
    def t = asList(title).first()
    if (t.mainTitle && !multipleValues(t.mainTitle)) {
        def norm = asList(t.mainTitle).first().replaceFirst(~/\s*(?<!\.)\.\s*$/, '')
        if (norm != t.mainTitle) {
            t.mainTitle = norm
            return true
        }
    }
    return false
}

boolean multipleValues(Object thing) {
    return asList(thing).size() > 1
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
    def mt = asList(work['hasTitle'])[0]?.mainTitle
    if (multipleValues(mt))
        return false

    def (title, languages) = splitTitleLanguages(asList(mt)[0])

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


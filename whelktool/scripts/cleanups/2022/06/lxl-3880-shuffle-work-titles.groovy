/**
 * The goal with this script is 1) to move translated (original) titles to translationOf and 2) to move out titles from
 * expressionOf and then remove the property.
 *
 * The script mainly moves titles from three different places:
 *  - instanceOf.hasTitle is moved to instanceOf.translationOf for all records where both instanceOf.hasTitle and
 *    instanceOf.translationOf are present.
 *  - instanceOf.expressionOf.hasTitle is moved to instanceOf.translationOf.hasTitle if instanceOf.translationOf is present,
 *    otherwise to instanceOf.hasTitle.
 *  - instanceOf.hasPart.hasTitle is moved to instanceOf.hasPart.translationOf.hasTitle if instanceOf.hasPart.translationOf is present.
 *
 * A few additional properties (if present) follow the title to the target entity (see moveAdditional method).
 *
 * If a move can't be executed due to unexpected data, the update won't be saved. The intended move is instead written to a report for manual handling.
 *
 * In case expressionOf is a linked entity, hasTitle and associated properties are copied to the target while the link remains in expressionOf.
 * In case expressionOf is a local entity, all its properties except @type and language are moved to the target (expressionOf is removed).
 * In case instanceOf.@type is Music or NotatedMusic nothing is moved to translationOf. The expressionOf content is always moved to instanceOf.
 *
 *
 * A couple normalizations are done on the mainTitle string whenever a title is moved:
 *  - Trailing period (not preceded by another period or a capital letter) is removed.
 *  - Language is removed from the string if the language can be identified in the target entity's language property.
 *    Example: mainTitle: "Haggada. Jiddisch & Hebreiska" --> mainTitle: Haggada, language: [[@id:https://id.kb.se/language/yid], [@id:https://id.kb.se/language/heb]]
 *
 */

import whelk.filter.LanguageLinker
import whelk.util.Statistics
import whelk.util.Unicode

import java.util.concurrent.ConcurrentLinkedQueue

moved = getReportWriter('moved.tsv')
propertyAlreadyExists = getReportWriter('property-already-exists.tsv')
brokenLinks = getReportWriter('broken-links.tsv')
multiple = getReportWriter('multiple.txt')
langDiff = getReportWriter('lang-diff.tsv')

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

def where = """
    collection = 'bib'
    and (
        data['@graph'][1]['instanceOf']['expressionOf'] is not null
        or (
            data['@graph'][1]['instanceOf']['hasTitle'] is not null 
            and data['@graph'][1]['instanceOf']['translationOf'] is not null
        )
    )
"""

def whereHasPart = """
    collection = 'bib'
    and data #>> '{@graph,1,instanceOf,hasPart}' like '%"translationOf"%' 
"""

selectBySqlWhere(where) { bib ->
    Map instance = bib.graph[1]
    def instanceOf = instance.find { it.key == INSTANCE_OF }
    def shortId = bib.doc.shortId

    Map work = asList(instanceOf.value)[0]

    def expressionOf = work.find { it.key == EXPRESSION_OF }

    def isMusic = work['@type'] in ['Music', 'NotatedMusic']

    if (!expressionOf) {
        if (isMusic) {
            return
        }
        def target = work.find { it.key == TRANSLATION_OF }
        // Move title and its associated properties from instanceOf to translationOf if translationOf exists
        if (moveProperty(shortId, HAS_TITLE, instanceOf, target) && moveAdditional(shortId, instanceOf, target)) {
            work.remove(HAS_TITLE)
            bib.scheduleSave()
        }
        return
    }

    if (multipleValues(expressionOf.value)) {
        multiple.println("$shortId\t$EXPRESSION_OF")
        return
    }

    // Move expressionOf content primarily to translationOf, otherwise to instanceOf
    def target = isMusic ? instanceOf : (work.find { it.key == TRANSLATION_OF } ?: instanceOf)

    Map hub = asList(expressionOf.value)[0]
    if (hub[ID]) {
        expressionOf.value = loadThing(hub[ID])
        if (!expressionOf.value) {
            brokenLinks.println([shortId, hub[ID]].join('\t'))
        }
        // Linked uniform work title in expressionOf: copy hasTitle and its associated properties to target
        else if (moveProperty(shortId, HAS_TITLE, expressionOf, target) && moveAdditional(shortId, expressionOf, target)) {
            bib.scheduleSave()
        }
    }
    // Move title from local expressionOf to target
    else if (moveProperty(shortId, HAS_TITLE, expressionOf, target)) {
        if (hub[LANGUAGE] && !handleLang(shortId, work, hub)) {
            // expressionOf.language differs from work.language and translation.language, abort
            return
        }

        hub.remove(HAS_TITLE)

        // Move remaining properties (except @type) to the same place as the title
        hub.removeAll {
            it.key == '@type' ?: moveProperty(shortId, it.key, expressionOf, target)
        }

        if (hub.isEmpty()) {
            work.remove(EXPRESSION_OF)
            bib.scheduleSave()
        }
    }
}

selectBySqlWhere(whereHasPart) { bib ->
    Map work = bib.graph[1][INSTANCE_OF]
    def id = bib.doc.shortId

    def failed = false
    def modified = false

    // Move title from hasPart to hasPart.translationOf if hasPart.translationOf exists
    work[HAS_PART].each { Map p ->
        def from = Map.entry(HAS_PART, p)
        def target = p.find { it.key == TRANSLATION_OF }
        if (!target) {
            return
        }
        if (moveProperty(id, HAS_TITLE, from, target) && moveAdditional(id, from, target)) {
            p.remove(HAS_TITLE)
            modified = true
        } else {
            failed = true
        }
    }

    if (modified && !failed) {
        bib.scheduleSave()
    }
}

boolean handleLang(String id, Map work, Map hub) {
    if (asList(work[LANGUAGE]) == [['@id': 'https://id.kb.se/language/und']] && hub[LANGUAGE]) {
        work[LANGUAGE] = hub[LANGUAGE]
    }

    def workLang = asList(work[LANGUAGE])
    def orig = asList(work[TRANSLATION_OF])
    def origLang = asList(orig[0]?[LANGUAGE])
    def hubLang = hub[LANGUAGE]

    def whichOne = (workLang + origLang).findAll { it[ID] in ambiguousLangIds }
    def diff = asList(hubLang).any { l ->
        def mappedLangs = asList(l.'@id' ? l : mapBlankLanguages([l], whichOne))
        !workLang.containsAll(mappedLangs) && !origLang.containsAll(mappedLangs)
    }

    if (!diff) {
        hub.remove(LANGUAGE)
        return true
    }

    def cols = [id, hubLang, workLang, origLang]
    if (hub[HAS_TITLE].toString() =~ /[Bb]ib[bel]/) {
        cols << hub[HAS_TITLE]
    }

    langDiff.println(cols.join('\t'))

    return false
}

boolean moveAdditional(String id, Map.Entry from, Map.Entry target) {
    def moveThese = ['musicKey', 'musicMedium', 'version', 'marc:version', 'marc:fieldref', 'legalDate', 'originDate']

    def f = from.value
    def additional = f.keySet().intersect(moveThese)

    if (additional) {
        for (String key in additional) {
            if (!moveProperty(id, key, from, target)) {
                return false
            }
        }
        f.removeAll { it.key in additional }
    }

    return true
}

boolean moveProperty(String id, String propToMove, Map.Entry from, Map.Entry target) {
    def f = asList(from.value)[0]

    if (f[propToMove] && target) {
        def whatMove = "${from.key} -> ${target.key}"

        if (multipleValues(target.value)) {
            multiple.println("$id\t$target.key")
            return false
        }

        def t = asList(target.value)[0]

        if (t[propToMove]) {
            propertyAlreadyExists.println([id, whatMove, propToMove, f[propToMove], t[propToMove]].join('\t'))
        } else {
            def cols

            if (propToMove == HAS_TITLE) {
                def movedLang = moveLanguagesFromTitle(f, asList(t[LANGUAGE]).findAll { it[ID] in ambiguousLangIds })
                def periodRemoved = removePeriod(f[HAS_TITLE])
                cols = [id, whatMove, HAS_TITLE, f[HAS_TITLE], (movedLang || periodRemoved)]
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

boolean removePeriod(title) {
    def t = asList(title).first()
    if (t.mainTitle && !multipleValues(t.mainTitle)) {
        // Also removes any remaining (trailing) whitespace.
        def norm = asList(t.mainTitle).first().replaceFirst(~/\s*(?<!(\.|\p{Lu}))\.\s*$/, '')
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
            'slovenski jezik'                 : 'slovenska',
            'samiska'                         : 'samiskt språk'

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


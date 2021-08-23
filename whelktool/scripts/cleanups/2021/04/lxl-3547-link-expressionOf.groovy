/**
 * Try to replace local instanceOf.expressionOf with links to "uniformWorkTitle" works.
 * 
 * originDate, marc:version and title (mainTitle, partName, partNumber, marc:formSubheading) have to match.
 * expressionOf with language can link to work with same language or no language
 * expressionOf without language can link to work without language
 * 
 * Languages in expressionOf.hasTitle.mainTitle are moved to expressionOf.language
 * "Sefer ha-ḥinukh. Engelska & hebreiska" --> "Sefer ha-ḥinukh", language: [[@id:https://id.kb.se/language/eng], [@id:https://id.kb.se/language/heb]]
 * 
 * Identical expressionOf that occur in multiple records are extracted into new "uniformWorkTitle" works.
 * (If NUM_OCCURENCES_EXTRACT or more found.)
 * 
 * Other instanceOf.expressionOf that only have ['@type', 'hasTitle'] or ['@type', 'hasTitle', 'language'} are 
 * converted to instanceOf.hasTitle 
 * 
 * See LXL-3547 for more info
 * 
 */


import org.apache.commons.lang3.StringUtils
import whelk.Document
import whelk.filter.LanguageLinker
import whelk.util.Statistics
import whelk.util.Unicode

import java.text.Normalizer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.regex.Pattern

int NUM_OCCURENCES_EXTRACT = 7  // ~300 new works

// expressionOf linked to a "uniformWorkTitle" work
PrintWriter linked = getReportWriter("linked.txt")
// Found potential matches to link to, but non with matching language. Left untouched.
PrintWriter noLang = getReportWriter("no-matching-language.txt")
// Found multiple potential matches to link to. Left untouched.
PrintWriter multiMatch = getReportWriter("multiple-matches.txt")
// Lists identical expressionOf found in multiple records 
PrintWriter sameExpr = getReportWriter("same-expression.txt")
// Found matches but only when ignoring primary contribution (0 results)
PrintWriter ignoredContribution = getReportWriter("ignored-primary-contribution.txt")
// Title from expressionOf move to work
PrintWriter movedTitles = getReportWriter("moved-titles.txt")
// The work having 'expressionOf' did not have all languages in 'expressionOf.language' in its 'language' 
PrintWriter otherExpressionLanguage = getReportWriter("other-expression-language.txt")
// Language parsed from 'mainTitle' of 'expressionOf' and moved to 'language', e.g. "Koranen. Danska & Arabiska"
languageFromTitle = getReportWriter("language-moved-from-title.txt")
// Comarison of instance and expressionOf titles, e.g.:
// e.g. l3wll6cx1gz8q7b	issuanceType:Serial	isTranslation:false	isSame:false	isPrefix:true	suffix: (Malmö)	I:Bostaden	E:Bostaden (Malmö)
compareTitles = getReportWriter("compare-titles.txt")
// Identical expressionOf that were extracted into new "uniformWorkTitle" works.
PrintWriter extracted = getReportWriter("extracted.txt")

languageLinker = buildLanguageMap()
Map a = (Map<String, List>) languageLinker.ambiguousIdentifiers
ambiguousLangNames = a.keySet()
ambiguousLangIds = a.values().flatten()

languageNames = languageLinker.map.keySet() + languageLinker.substitutions.keySet() + languageLinker.ambiguousIdentifiers.keySet()
uniformWorks = getUniformWorks()

def notLinkedExpr = new ConcurrentHashMap<Map, ConcurrentLinkedQueue<String>>()

// Generic titles that should not be extracted inte new works. e.g. "Annual report"
def genericTitles = new File(scriptDir, 'lxl-3547-link-expressionOf-generic-titles.txt').readLines().collect( Norm.&normalize  )

// ############################################################################
// Try to link
selectBySqlWhere("data#>>'{@graph,1,instanceOf,expressionOf}' is not null") { bib ->
    Map work = getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf'])
    List<Map> expressionOf = asList(getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'expressionOf']))

    if (!work || !expressionOf) {
        return
    }
    
    boolean modified = false
    expressionOf.each { Map e -> 
        if (e['@id']) {
            return
        }

        incrementStats('shape', new TreeSet<>(e.keySet()))
        
        if (!e['hasTitle']) {
            return
        }

        List whichOne = asList(work.language).findAll { it['@id'] in ambiguousLangIds }
        
        if (moveLanguagesFromTitle(bib.doc.shortId, e, whichOne)) {
            bib.scheduleSave()
        }

        // language must contain all languages in expressionOf.language, otherwise abort
        // Work
        //  language
        //  expressionOf
        //    language
        if (e.language && !asList(work.language).containsAll(mapBlankLanguages(asList(e.language), whichOne))) {
            otherExpressionLanguage.println("${bib.doc.shortId} E: ${toString(e)} W: ${toString(work)}" )
            return
        }

        // there is always exactly one title
        def cmpMap = Norm.cmpTitle(asList(e['hasTitle']).first()) + Norm.cmpOther(e)
        getPrimaryContributionString(work)?.with {cmpMap += ['primaryContribution': it] }
        Collection works = uniformWorks.getOrDefault(cmpMap, Collections.emptyList())
        
        if (!works && cmpMap.primaryContribution) {
            // try without primary contribution
            works = uniformWorks.getOrDefault(cmpMap - ['primaryContribution': ''], Collections.emptyList())
            if (works) {
                ignoredContribution.println("${bib.doc.id} ${toString(e)} --> ${works.collect {it['@id'] + " " + toString(it)}}")
            }
        }
        
        incrementStats('number of works found for expressionOf', works.size())
        
        if (works) {
            def worksLang = compatibleLanguages(e, works)
            incrementStats('number of works found for expressionOf, filtered on lang', worksLang.size())

            if (worksLang.size() == 0) {
                noLang.println("${bib.doc.id} ${toString(e)} ??? ${works.collect {it['@id'] + " " + toString(it)}}")
            }
            else if (worksLang.size() == 1) {
                linked.println("${bib.doc.id} ${toString(e)} --> ${worksLang[0].with {it['@id'] + " " + toString(it)}}")
                e.clear()
                e['@id'] = worksLang[0]['@id']
                modified |= true
            }
            else {
                multiMatch.println("${bib.doc.id} ${toString(e)} ??? ${worksLang.collect {it['@id'] + " " + toString(it)}}")
            }
        }
        else {
            Map ee = Document.deepCopy(e)
            
            asList(ee.hasTitle).each { Map title ->
                title.keySet().each {
                    if (!it.startsWith('@')) {
                        title[it] = Unicode.trimNoise(title[it])
                    }
                }
            }
            
            getPrimaryContributionString(work)?.with {ee += ['primaryContribution': it] }
            notLinkedExpr.computeIfAbsent(ee, { new ConcurrentLinkedQueue<String>() })
            notLinkedExpr.get(ee).add(bib.doc.shortId)
        }
    }
    
    if (modified) {
        bib.scheduleSave()
    }
}

// ############################################################################
// Find the expressionOfs should that should be extracted to new works 
List<String> unmatchedIds = []
Map toBeExtracted = [:]
notLinkedExpr.each {key, ids ->
    ids.each {
        incrementStats('same expr', toString(key), it)
    }

    if (ids.size() <= 2) {
        unmatchedIds.add(ids.poll())
    }
    else if (ids.size() < NUM_OCCURENCES_EXTRACT) {
        // Don't touch these for now
        //unmatchedIds.add(ids.poll())
        sameExpr.println(ids.size() + " " + toString(key))
    }
    else {
        if (Norm.normalize(asList(key['hasTitle'])?.first().mainTitle) in genericTitles) {
            sameExpr.println(ids.size() + " " + toString(key) + " [GENERIC TITLE]")
            unmatchedIds.add(ids.poll())
        }
        else {
            sameExpr.println(ids.size() + " " + toString(key) + " [E]")
            toBeExtracted[key] = ids
        }
    }
}

// ############################################################################
// Convert instanceOf.expressionOf to instanceOf.hasTitle 
def title = ['@type', 'hasTitle'] as Set
def titleAndLang = ['@type', 'hasTitle', 'language'] as Set
selectByIds(unmatchedIds) { bib ->
    Map instance = bib.doc.data['@graph'][1]
    Map work = instance.instanceOf
    List<Map> expressionOf = asList(work.expressionOf)

    if (!work || !expressionOf) {
        return
    }
    
    List<Map> remove = []
    expressionOf.each { Map expression ->
        if (expression.keySet() == title || expression.keySet() == titleAndLang) {
            if (work.hasTitle) {
                if (work.hasTitle == expression.hasTitle) {
                    remove.add(expression)
                    bib.scheduleSave()
                    movedTitles.println("$bib.doc.shortId SAME hasTitle W: ${toString(work)} E: ${toString(expression)}")
                    incrementStats('unique titles', 'work already had same title')
                }
                else {
                    movedTitles.println("$bib.doc.shortId ALREADY hasTitle W: ${toString(work)} E: ${toString(expression)}")
                    incrementStats('unique titles', 'work already has title')
                    printTitles(bib.doc.shortId, instance, expression)
                }
            }
            else {
                work.hasTitle = expression.hasTitle
                remove.add(expression)
                bib.scheduleSave()
                movedTitles.println("$bib.doc.shortId MOVED hasTitle E/W: ${toString(work)} I: ${toString(instance)}")
                incrementStats('unique titles', 'moved')
                printTitles(bib.doc.shortId, instance, expression)
            }
        }
        else {
            incrementStats('unique titles - bad shape', expression.keySet())
        }
    }
    
    expressionOf.removeAll(remove)
    if (!expressionOf) {
        work.remove('expressionOf')
    }
}

// ############################################################################
// Extract new works
toBeExtracted.each { Map work, Collection<String> ids ->
    def data =
            ["@graph": [
                    [
                            "@type"                 : "Record",
                            "@id"                   : "TEMPID",
                            "mainEntity"            : ["@id": "TEMPID#it"],
                            "descriptionConventions": [["@id": "https://id.kb.se/marc/CatalogingRulesType-c"]],
                            "descriptionLanguage"   : ["@id": "https://id.kb.se/language/swe"],
                    ],
                    work + [
                            "@type"       : "Work",
                            "@id"         : "TEMPID#it",
                            "inCollection": [["@id": "https://id.kb.se/term/uniformWorkTitle"]]
                    ]
            ]]

    def item = create(data)
    selectFromIterable([item], { newlyCreatedItem ->
        newlyCreatedItem.scheduleSave()
    })
    String newId = item.graph[1]['@id']
    
    selectByIds(ids) { bib ->
        bib.graph[1].instanceOf.expressionOf = [ '@id' : newId ] 
        bib.scheduleSave()
    }
    extracted.println("$newId ${toString(work)} <-- $ids")
}

// ############################################################################
// ############################################################################

Collection<Map> compatibleLanguages(Map expressionOf, Collection<Map> works) {
    if (lang(expressionOf)) {
        def sameLang = works.findAll{ lang(it) == lang(expressionOf)}
        return sameLang ?: works.findAll{ lang(it) == Collections.emptySet() }
    }
    else {
        return works.findAll{ lang(it) == Collections.emptySet() }
    }
}


private Map<String, Collection<Map>> getUniformWorks() {
    def q = [
            'inCollection.@id': ['https://id.kb.se/term/uniformWorkTitle'],
            '_sort'           : ['@id']
    ]
    
    def works = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Map>>()
    selectByIds(queryIds(q).collect()) { doc ->
        Map work = getPathSafe(doc.doc.data, ['@graph', 1])
        
        def titles = (asList(work.hasTitle) + asList(work.hasVariant).collect{ asList(it['hasTitle']) }.flatten())
        titles = titles.collect{ Norm.cmpTitle(it) }
        
        def otherFields = Norm.cmpOther(work)
        getPrimaryContributionString(work)?.with {otherFields += ['primaryContribution': it] }
        
        def cmpMaps = titles.collect{otherFields + it} as Set
        cmpMaps.each { map ->
            works.computeIfAbsent(map, { new ConcurrentLinkedQueue<Map>() })
            works.get(map).add(work)
        }
        
    }
    return works
}

private String toString(Map work) {
    def keys = (Norm.TITLE_KEYS.collect {['hasTitle', 0] + it } + Norm.CMP_KEYS) 
    def props = keys.collect {getPathSafe(work, asList(it)) }
    def langcode = lang(work).collect{ (it['@id'] ?: asList((it['label'] ?: '')).first()).split('/').last() }
    def contribution = asList(work['primaryContribution']) + asList(getPrimaryContributionString(work))
    return (props + langcode + contribution).grep().join(' · ')
}

private Object getPathSafe(item, path, defaultTo = null) {
    for (p in path) {
        if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}

private Set lang(Map work) {
    (mapBlankLanguages(asList(work.language)) + mapBlankLanguages(asList(work.associatedLanguage))) as Set
}

private String getPrimaryContributionString(Map work) {
    List<Map> a = asList(work.contribution).find { it['@type'] == 'PrimaryContribution' }?.with { return asList(it.agent) }
    if (a) {
        def agent = a.first()
        if (agent['@id']) {
            agent = loadThing(agent['@id'])
        }
        
        StringBuilder b = new StringBuilder()
        for (def k : ['name', 'givenName', 'familyName']) {
            if (agent.containsKey(k)) {
                b.append(agent[k]).append(' ')
            }
        }
        return (Norm.normalize(b.toString()))
    }
    return null
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

class Norm {
    static final Pattern UNICODE_MARK = Pattern.compile('\\p{M}')

    // these are the ones found in the data. ignore @type and marc:nonfilingChars
    static final Set TITLE_KEYS = ['mainTitle', 'partName', 'partNumber', 'marc:formSubheading'] as Set

    static final Set CMP_KEYS = ['originDate', 'marc:version'] as Set 
    
    static def noise = [",", '"', "'", '[', ']', ',', '.', '.', ':', ';', '-', '(', ')', ' the ', '-', '–', '+', '!', '?'].collectEntries { [it, ' '] }

    static Map cmpOther(Map work) { 
        normalize(work.findAll {it.key in CMP_KEYS})
    }
    
    static Map cmpTitle(Map title) {
        normalize(title.findAll {it.key in TITLE_KEYS})
    }
        
    static Map normalize(Map map) {
        map
                .collectEntries {
                    [(it.key): it.value instanceof List
                            ? ((List) it.value).collect{ normalize(it) }.sort()
                            :  normalize(it.value)
                    ]
                }
    }
    
    static String normalize(String s) {
        s = " $s "
        s = toLowerCase(s)
        s = s.replace(noise)
        s = s.replace('æ', 'ae')
        s = StringUtils.normalizeSpace(s)
        s = asciiFold(s)
        s = Unicode.normalizeForSearch(s)
        return s
    }

    static String toLowerCase(String s) {
        // Don't touch abbreviations, e.g. LOU
        boolean allUpperCase = s.toUpperCase(Locale.ROOT) == s
        return allUpperCase ? s : s.toLowerCase(Locale.ROOT)
    }
    
    static String asciiFold(String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll(UNICODE_MARK, '')
    }
}

void printTitles(String id, Map instance, Map expressionOf) {
    String i = titleStr(instance)
    String e = titleStr(expressionOf)
    boolean isSame = i == e
    boolean isPrefix = e.startsWith(i) && !isSame
    String suffix = isPrefix ? e.substring(i.size()) : ""
    boolean isTranslation = instance.instanceOf?.containsKey('translationOf')
    compareTitles.println([
            id,
            "issuanceType:${instance.issuanceType}",
            "isTranslation:${isTranslation}",
            "isSame:${isSame}",
            "isPrefix:${isPrefix}",
            "suffix:${suffix}",
            "I:${i}",
            "E:${e}"
    ].join('\t'))
}

private String titleStr(Map thing) {
    def keys = Norm.TITLE_KEYS.collect {['hasTitle', 0] + it }
    def props = keys.collect {getPathSafe(thing, asList(it)) }
    return props.grep().join(' ')
}

// Handle e.g. { "@type": "Language", "label": ["English & Tamil."] }
private List mapBlankLanguages(List languages, List whichLanguageVersion = []) {
    if (languages.size() == 1 && languages[0].label) {
        String label = asList(languages[0].label).first().toLowerCase()
        // to be able to map e.g. "Greek" to modern or classical Greek (LanguageLinker will use an existing linked sibling to decide when ambiguous)
        List copy = new ArrayList( 
                ambiguousLangNames.any{ label.contains(it) } 
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
boolean moveLanguagesFromTitle(String id, Map work, List whichLanguageVersion = []) {
    def (title, languages) = splitTitleLanguages(asList(work['hasTitle'])?.first().mainTitle)
    
    if (!languages) {
        return false
    }

    def l = languages.collect{ ['@type': 'Language', 'label': it] }
    
    if (work.language) {
        l += work.language
    }
    
    if (languages.any{ it in ambiguousLangNames }) {  
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
    
    languageFromTitle.println("$id ${work.hasTitle.mainTitle} --> $title $l")
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
            'nubiska'                         : 'nubiska språk'

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
    languages.forEach({l -> linker.addDefinition(l) } )

    linker.addSubstitutions(substitutions())
    linker.addMapping('grekiska', 'https://id.kb.se/language/gre')
    linker.addMapping('grekiska', 'https://id.kb.se/language/grc')
    linker.addMapping('greek', 'https://id.kb.se/language/gre')
    linker.addMapping('greek', 'https://id.kb.se/language/grc')

    linker.addMapping('syriska', 'https://id.kb.se/language/syc')
    linker.addMapping('syriska', 'https://id.kb.se/language/syr')

    return linker
}

/*
broken, fix manually:
   1 [@type, subtitle, mainTitle, marc:formSubheading]            [2btjtjz00hlpvk19]
   1 [@type, subtitle, mainTitle]                                 [8m6b5z206blc8p4n]
 */
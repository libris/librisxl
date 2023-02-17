import whelk.util.Unicode
import whelk.util.DocumentUtil

PrintWriter unhandledScript = getReportWriter("unhandled-script.txt")
PrintWriter moved = getReportWriter("moved.txt")
PrintWriter skipped = getReportWriter("skipped.txt")
Util u = new Util(getWhelk())

MOVEABLE = [
        'subtitle' : 'titleRemainder',
        'qualifier': 'subtitle',
        'partName' : 'partNumber'
]

def where = """
    collection = 'bib'
    AND (data #>> '{@graph,0,technicalNote}' ILIKE '%translitter%'
        OR data #>> '{@graph,0,technicalNote}' ILIKE '%transkriber%')
"""

selectByIds(new File(scriptDir, 'ids.txt').readLines()) { bib ->
//selectBySqlWhere(where) {
    //println(it.doc.shortId)
    //return
    
    def thing = bib.graph[1]
    List hasTitle = thing['hasTitle']
    if (!hasTitle) {
        incrementStats("skipped", "no hasTitle")
        return
    }

    def langId = thing.instanceOf.subMap('language').with {
        u.langLinker.linkAll(it)
        // TODO: only one language?
        asList(it.language).findResult { it['@id'] } ?: 'https://id.kb.se/language/und'
    }

    def titles = hasTitle.findAll { it['@type'] == 'Title' }
    def variants = hasTitle.findAll { it['@type'] == 'VariantTitle' }
    def nonLatinVariants = []
    for (Map variant : variants) {
        var script = guessScript(variant)
        if (script.isPresent()) {
            if (script.get() != 'Latn') {
                nonLatinVariants << variant
            }
        }
        else {
            if (langId = 'https://id.kb.se/language/jpn') {
                nonLatinVariants << variant
            } else {
                unhandledScript.println("${bib.doc.shortId} ${langId} ${variant}")
                incrementStats("skipped", "unknown script")
                return
            }
        }
    }
    
    if (!titles || !nonLatinVariants || titles.size() > 1 || nonLatinVariants.size() > 1) {
        def m = "Title ${titles.size()}, VariantTitle ${nonLatinVariants.size()}, hasBib880 ${thing['marc:hasBib880'] != null}"
        incrementStats("skipped", m)
        if (!(titles.size() == 1 && nonLatinVariants.size() == 0)) {
            skipped.println("${bib.doc.shortId} $m")
        }
        return 
    }
    
    Map title = titles[0]
    Map variant = nonLatinVariants[0]

    def langTag = u.langToLangTag[langId]
    def tLangCandidates = u.langToTLang[langId]
    if (!tLangCandidates) {
        incrementStats('Missing tLang', langId)
        incrementStats('Missing tLang - num lang', asList(thing.instanceOf.language).size())
        return
    }
    def tLangTag = tLangCandidates.size() == 1 ? tLangCandidates[0].code : chooseTLang(variant, tLangCandidates)
    if (!tLangTag) {
        incrementStats('Missing tLangTag', langId)
        return
    }
    
    Closure merge
    merge = { Map romanized, Map originalScript ->
        def remaining = []
        def movedTo = []
        def result = [:]
        def keys = originalScript.keySet() - ['@type', 'marc:nonfilingChars']
        for (k in keys) {
            if (!romanized[k]) {
                if (MOVEABLE[k] && romanized[MOVEABLE[k]] && !originalScript[MOVEABLE[k]]) {
                    // never more than one
                    def r = asList(romanized[MOVEABLE[k]]).first()
                    def o = asList(originalScript[k]).first()
                    incrementStats('Moved', "$k -> ${MOVEABLE[k]}")
                    moved.println("${bib.doc.shortId} $k -> ${MOVEABLE[k]}\n  $o\n  $r\n")
                    movedTo << MOVEABLE[k]
                    result[MOVEABLE[k]] = [
                            (tLangTag) : r,
                            (langTag) : o
                    ]
                }
                else {
                    remaining << k    
                }
                continue
            }
            
            if (originalScript[k] instanceof List || romanized[k] instanceof List) {
                def o = asList(originalScript[k])
                def r = asList(romanized[k])
                if (r.size() == o.size()) {
                    if (u.langAliases[k]) {
                        if (r.size() != 1) {
                            incrementStats("Multiple language tagged strings", k)
                            result[u.langAliases[k]] = [
                                    (tLangTag) : r,
                                    (langTag) : o
                            ]
                        }
                        else {
                            result[u.langAliases[k]] = [
                                    (tLangTag) : r[0],
                                    (langTag) : o[0]
                            ]
                        }
                    }
                    else {
                        result[k] = []
                        for (int i = 0 ; i < r.size() ; i++) {
                            def (res, rest) = merge(r[i], o[i])
                            result[k] << res
                            remaining.addAll(rest)
                        }
                    }
                }
                else {
                    incrementStats('Incompatible', k)
                    remaining << k
                }
            }
            else if (originalScript[k] instanceof Map) {
                def (r, rest) = merge(romanized[k], originalScript[k])
                result[k] = r
                remaining.addAll(rest)
            }
            else if (originalScript[k] instanceof String) {
                result[u.langAliases[k]] = [
                        (tLangTag) : romanized[k],
                        (langTag) : originalScript[k]
                ]
            }
            else {
                throw new RuntimeException("Unexpected type: " + originalScript[k])
            }
        }
        result['@type'] = romanized['@type']
        result.putAll(romanized.subMap(romanized.keySet() - originalScript.keySet() - movedTo))

        return [result, remaining]
    }
    
    def (result, remaining) = merge(title, variant)
    if (remaining) {
        incrementStats('Unhandled properties in variant', remaining)
        return 
    }

    hasTitle.remove(title)
    hasTitle.remove(variant)
    hasTitle.add(0, result)

    bib.scheduleSave()
}

def chooseTLang(Map m, List tLangs) {
    def scriptUri = 'https://id.kb.se/i18n/script/' + guessScript(m).orElse(null)
    return tLangs.find { it.fromLangScript == scriptUri }?.code
}

def guessScript(Map m) {
    StringBuilder s = new StringBuilder()
    DocumentUtil.traverse(m) { value, path ->
        if (value instanceof String && path.last() != '@type') {
            s.append(value)
        }
        return DocumentUtil.NOP
    }
    return Unicode.guessIso15924ScriptCode(s.toString())
}

def looksLikeScript(String s, String scriptCode) {
    Unicode.guessIso15924ScriptCode(s).map{ it == scriptCode }.orElse(false)
}

def getWhelk() {
    def whelk = null

    selectByIds(['https://id.kb.se/marc']) {
        whelk = it.whelk
    }

    return whelk
}

class Util {
    def langLinker

    Map tLangCodes
    Map langToTLang
    Map langAliases
    Map langAliasesReversed
    Map langToLangTag

    Util(whelk) {
        this.langLinker = whelk.languageResources.languageLinker
        this.tLangCodes = getTLangCodes(whelk.languageResources.transformedLanguageForms)
        this.langToTLang = getLangToTLangs(tLangCodes)
        this.langAliases = whelk.jsonld.langContainerAlias
        this.langAliasesReversed = langAliases.collectEntries { k, v -> [v, k] }
        this.langToLangTag = whelk.languageResources.languages
                .findAll { it.value.langTag }.collectEntries { k, v ->  [k, v.langTag] }
    }

    static Map<String, Map> getTLangCodes(Map transformedLanguageForms) {
        return transformedLanguageForms
                .collectEntries { k, v ->
                    def data = [:]
                    if (v.inLanguage) {
                        data.inLanguage = v.inLanguage['@id']
                    }
                    if (v.fromLangScript) {
                        data.fromLangScript = v.fromLangScript['@id']
                    }
                    [v.langTag, data]
                }
    }

    static Map<String, List> getLangToTLangs(Map tLangs) {
        def langToTLangs = [:]

        tLangs.each { code, data ->
            def entry = langToTLangs.computeIfAbsent(data.inLanguage, x -> [])
            entry.add(data.plus(['code': code]))
        }

        return langToTLangs
    }
}
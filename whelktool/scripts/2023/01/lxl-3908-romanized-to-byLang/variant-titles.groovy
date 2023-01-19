import whelk.filter.LanguageLinker
import whelk.util.Romanizer

import java.util.regex.Pattern

Util u = new Util(getWhelk())

def where = """
    collection = 'bib'
    AND (data #>> '{@graph,0,technicalNote}' ILIKE '%translitter%'
        OR data #>> '{@graph,0,technicalNote}' ILIKE '%transkriber%')
"""

selectBySqlWhere(where) {
    def thing = it.graph[1]
    def hasTitle = thing['hasTitle']
    if (!hasTitle) {
        return
    }

    def lang = thing.instanceOf.subMap('language').with {
        u.langLinker.linkAll(it)
        asList(it.language).findResult { it['@id'] } ?: 'https://id.kb.se/language/und'
    }

    def titles = hasTitle.findAll { it['@type'] == 'Title' }
    def variants = hasTitle.findAll { it['@type'] == 'VariantTitle' }
    if (!titles || !variants) {
        return
    }
    if (titles.size() > 1) {
        return
    }

    def title = titles[0]

    def langTag = u.langToLangTag[lang]
    def tLangCandidates = u.langToTLang[lang]
    if (!tLangCandidates) {
        incrementStats('Missing tLang', lang)
        return
    }
    def tLang = tLangCandidates.size() == 1 ? tLangCandidates[0].code : null

    def byLang = [:]

    variants.each {
        def remaining = [] as Set
        it.each { k, v ->
            // TODO: v can be Map...example r7pccfs7pd77q83s
            def compact = v.toString().replaceAll(/[^\p{L}]/, '')
            if (u.langAliases[k] && compact && !looksLikeScript(compact, ~/\p{IsLatin}/)) {
                tLang = tLang ?: chooseTLang(compact, tLangCandidates)
                if (!tLang) {
                    incrementStats('Ambiguous tLang, could not decide which to use', lang)
                    return
                }
                incrementStats('tLangs', tLang)
                def entry = byLang.computeIfAbsent(u.langAliases[k], x -> [:])
                entry[langTag] = entry[langTag] ? asList(entry[langTag]) + v : v
                entry[tLang] = entry[tLang] ? asList(entry[tLang]) + title[k] : title[k]
            } else {
                remaining.add(k)
            }
        }
        remaining.remove('@type')
        if (it.subMap(remaining) != title.subMap(remaining)) {
            incrementStats('Unhandled properties in variant', remaining)
            return
        } else {
            hasTitle.remove(it)
        }
    }

    if (byLang) {
        hasTitle.find { it['@type'] == 'Title' }.with { t ->
            t.putAll(byLang)
            byLang.each { k, v ->
                if (v instanceof List && v.size() > 1) {
                    incrementStats('Stats', 'Multiple variants')
                }
                t.remove(u.langAliasesReversed[k])
            }
        }
        it.scheduleSave()
    }
}

def chooseTLang(String s, List tLangs) {
    Map scriptToRegex =
            [
                    'https://id.kb.se/i18n/script/Cyrl': ~/\p{IsCyrillic}/,
                    'https://id.kb.se/i18n/script/Arab': ~/\p{IsArabic}/,
                    'https://id.kb.se/i18n/script/Deva': ~/\p{IsDevanagari}/,
                    'https://id.kb.se/i18n/script/Beng': ~/\p{IsBengali}/,
                    'https://id.kb.se/i18n/script/Thai': ~/\p{IsThai}/,
                    'https://id.kb.se/i18n/script/Mymr': ~/\p{IsMyanmar}/,
                    'https://id.kb.se/i18n/script/Sinh': ~/\p{IsSinhala}/
            ]

    for (entry in scriptToRegex) {
        if (looksLikeScript(s, entry.value)) {
            return tLangs.find { it.fromLangScript == entry.key }?.code
        }
    }

    return null
}

def looksLikeScript(String s, Pattern p) {
    return s.findAll { it ==~ p }.size() / s.size() > 0.5
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
        this.langLinker = getLangLinker(whelk.normalizer.normalizers)
        this.tLangCodes = Collections.synchronizedMap(getTLangCodes(whelk.normalizer.normalizers))
        this.langToTLang = Collections.synchronizedMap(getLangToTLangs(tLangCodes))
        this.langAliases = Collections.synchronizedMap(whelk.jsonld.langContainerAlias)
        this.langAliasesReversed = Collections.synchronizedMap(langAliases.collectEntries { k, v -> [v, k] })
        this.langToLangTag = Collections.synchronizedMap(getLangTags(whelk.normalizer.normalizers))
    }

    static Map<String, Map> getTLangCodes(normalizers) {
        return normalizers.find { it.normalizer instanceof Romanizer }
                .normalizer
                .tLangs
                .collectEntries {
                    def code = it['@id'].split('/').last()
                    def data = [:]
                    if (it.inLanguage) {
                        data.inLanguage = it.inLanguage['@id']
                    }
                    if (it.fromLangScript) {
                        data.fromLangScript = it.fromLangScript['@id']
                    }
                    [code, data]
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

    static Map<String, String> getLangTags(normalizers) {
        return normalizers.find { it.normalizer instanceof Romanizer }
                .normalizer
                .langTags
    }

    static LanguageLinker getLangLinker(normalizers) {
        return normalizers.find { it.normalizer instanceof LanguageLinker }
                .normalizer
    }
}
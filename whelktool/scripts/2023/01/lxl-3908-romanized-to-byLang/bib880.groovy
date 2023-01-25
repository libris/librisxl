import whelk.filter.LanguageLinker
import whelk.util.Romanizer
import whelk.util.DocumentUtil

import java.util.regex.Pattern

Util u = new Util(getWhelk())

def where = """
    collection = 'bib'
    AND data#>>'{@graph,1,${u.HAS_BIB880}}' IS NOT NULL
"""

selectBySqlWhere(where) {
    def (record, thing) = it.graph

    def lang = thing.instanceOf.subMap('language').with {
        u.langLinker.linkAll(it)
        asList(it.language).findResult { it[u.ID] } ?: 'https://id.kb.se/language/und'
    }

    def langTag = u.langToLangTag[lang]
    if (!langTag) {
        incrementStats('Missing langTag', lang)
        return
    }

    def hasBib880 = thing.remove(u.HAS_BIB880)

    def tLangCandidates = u.langToTLang[lang]
    if (!tLangCandidates) {
        incrementStats('Missing tLang', lang)
        return
    }
    def tLang = tLangCandidates.size() == 1 ? tLangCandidates[0].code : null

    def failed = false

    Map bib880ByField = [:]
    asList(hasBib880).each { bib880 ->
        def partList = asList(bib880[u.PART_LIST])
        if (!partList) {
            incrementStats('edge cases', 'missing partlist')
            failed = true
            return
        }
        def linkField = partList[0][u.FIELDREF]
        if (!(linkField =~ /^\d{3}-\d{2}/)) {
            incrementStats('edge cases', 'missing or malformed fieldref')
            failed = true
            return
        }
        if (!tLang) {
            // Ambiguous tLang, try decide by examining script type of the original strings
            def strings = partList.drop(1)*.values().flatten()
            for (String s : strings) {
                def compact = s.replaceAll(/[^\p{L}]/, '')
                if (compact) {
                    tLang = chooseTLang(compact, tLangCandidates)
                }
                if (tLang) {
                    break
                }
            }
        }
        def parts = linkField.split('-')
        def tag = parts[0]
        def seqNum = parts[1].take(2)
        if (seqNum != '00') {
            def entry = bib880ByField.computeIfAbsent(tag, s -> [])
            entry.add(['ref': '880-' + seqNum, 'bib880': bib880])
        }
    }

    if (!failed && !tLang) {
        incrementStats('Ambiguous tLang, could not decide which to use', lang)
        return
    }

    Map bib880Map = [:]
    Map sameField = [:]

    bib880ByField.each { tag, data ->
        def converted = null
        try {
            def marcJson = data.collect { bib880ToMarcJson(it.bib880, u) }
            def marc = [leader: "00887cam a2200277 a 4500", fields: marcJson]
            converted = u.converter.runConvert(marc)
        } catch (Exception e) {
            incrementStats('edge cases', 'failed conversion')
            failed = true
            return
        }

        def refs = data.collect {
            bib880Map[it.ref] = converted
            it.ref
        } as Set

        refs.each {
            sameField[it] = refs
        }
    }

    Set handled = []
    def handle880Ref = { ref, path ->
        if (sameField[ref]?.intersect(handled)) {
            handled.add(ref)
            return new DocumentUtil.Remove()
        }
        def converted = bib880Map[ref]
        if (converted) {
            mergeLanguage(converted['@graph'][1], thing, u, langTag, tLang)
            handled.add(ref)
            return new DocumentUtil.Remove()
        }
    }

    if (!failed) {
        u.FIELDREFS.each {
            DocumentUtil.findKey(thing, it, handle880Ref)
        }
        it.scheduleSave()
    }
}

def getWhelk() {
    def whelk = null

    selectByIds(['https://id.kb.se/marc']) {
        whelk = it.whelk
    }

    return whelk
}

void mergeLanguage(Map thing, Map converted, Util u, String langTag, String tLang) {
    def nonByLangPaths = []
    DocumentUtil.findKey(converted, u.langAliases.keySet()) { value, path ->
        nonByLangPaths.add(path.collect())
        return
    }

    nonByLangPaths.each { path ->
        def containingObject = getAtPath(thing, path.dropRight(1))
        if (!containingObject) {
            //TODO: No romanized version seems to exists...
            return
        }

        def byLang = [:]
        asList(containingObject).each {
            it.each { k, v ->
                if (k == path.last()) {
                    def byLangProp = u.langAliases[k]
                    byLang[byLangProp] =
                            [
                                    (langTag): getAtPath(converted, path),
                                    (tLang)  : v
                            ]
                }
            }
            it.putAll(byLang)
        }
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
                    'https://id.kb.se/i18n/script/Sinh': ~/\p{IsSinhala}/,
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

Map bib880ToMarcJson(Map bib880, Util u) {
    def parts = asList(bib880[u.PART_LIST])
    def tag = parts[0][u.FIELDREF].split('-')[0]
    return [(tag): [
            (u.IND1)     : bib880["$u.BIB880-i1"],
            (u.IND2)     : bib880["$u.BIB880-i2"],
            (u.SUBFIELDS): parts[1..-1].collect {
                def subfields = it.collect { key, value ->
                    [(key.replace('marc:bib880-', '')): value]
                }
                return subfields.size() == 1 ? subfields[0] : subfields
            }
    ]]
}

class Util {
    def converter
    def langLinker

    Map tLangCodes
    Map langToTLang
    Map langAliases
    Map langToLangTag

    static final String HAS_BIB880 = 'marc:hasBib880'
    static final String BIB880 = 'marc:bib880'
    static final String PART_LIST = 'marc:partList'
    static final String FIELDREF = 'marc:fieldref'
    static final String SUBFIELDS = 'subfields'
    static final String IND1 = 'ind1'
    static final String IND2 = 'ind2'
    static final String ID = '@id'

    List FIELDREFS = [FIELDREF, 'marc:bib035-fieldref', 'marc:bib041-fieldref',
                      'marc:bib250-fieldref', 'marc:hold035-fieldref']

    Util(whelk) {
        this.converter = whelk.marcFrameConverter
        this.langLinker = getLangLinker(whelk.normalizer.normalizers)
        this.tLangCodes = Collections.synchronizedMap(getTLangCodes(whelk.normalizer.normalizers))
        this.langToTLang = Collections.synchronizedMap(getLangToTLangs(tLangCodes))
        this.langAliases = Collections.synchronizedMap(whelk.jsonld.langContainerAlias)
        this.langToLangTag = Collections.synchronizedMap(getLangTags(whelk.normalizer.normalizers))
    }

    static Map<String, Map> getTLangCodes(normalizers) {
        return normalizers.find { it.normalizer instanceof Romanizer }
                .normalizer
                .tLangs
                .collectEntries {
                    def code = it[ID].split('/').last()
                    def data = [:]
                    if (it.inLanguage) {
                        data.inLanguage = it.inLanguage[ID]
                    }
                    if (it.fromLangScript) {
                        data.fromLangScript = it.fromLangScript[ID]
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
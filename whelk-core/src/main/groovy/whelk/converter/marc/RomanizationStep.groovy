package whelk.converter.marc

import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.transform.NullCheck
import whelk.filter.LanguageLinker
import whelk.util.DocumentUtil

import static whelk.Document.deepCopy
import static whelk.JsonLd.asList

class RomanizationStep extends MarcFramePostProcStepBase {
    
    @CompileStatic
    @NullCheck(includeGenerated = true)
    static class LanguageResources {
        LanguageLinker languageLinker
        List<Map> languages
        Map transformedLanguageForms
        Map scripts
    }
    
    MarcFrameConverter converter
    LanguageResources languageResources

    Map tLangCodes
    Map langIdToLangTag
    Map langAliases
    Map byLangToBase
    Map langToTLang

    private static final String TARGET_SCRIPT = 'Latn'
    
    // Note: MARC standard allows ISO 15924 in $6 but Libris practice doesn't
    private static final Map MARC_SCRIPT_CODES = 
            [
                    'Arab': '/(3/r',
                    'Cyrl': '/(N',
                    'Cyrs': '/(N',
                    'Grek': '/(S',
                    'Hang': '/$1',
                    'Hani': '/$1',
                    'Hans': '/$1',
                    'Hant': '/$1',
                    'Hebr': '/(2/r'
            ]
    
    String OG_MARK = '**OG**'
    
    String HAS_BIB880 = 'marc:hasBib880'
    String BIB880 = 'marc:bib880'
    String PART_LIST = 'marc:partList'
    String FIELDREF = 'marc:fieldref'
    String SUBFIELDS = 'subfields'
    String IND1 = 'ind1'
    String IND2 = 'ind2'

    String BIB035_REF = 'marc:bib035-fieldref'
    String BIB041_REF = 'marc:bib041-fieldref'
    String BIB250_REF = 'marc:bib250-fieldref'
    String HOLD035_REF = 'marc:hold035-fieldref'
    
    List FIELD_REFS = [FIELDREF, BIB035_REF, BIB041_REF, BIB250_REF, HOLD035_REF]

    void modify(Map record, Map thing) {
        if (!languageResources)
            return
        
        // TODO: Do we really want to remove everything? What about "00" fields?
        // https://katalogverk.kb.se/katalogisering/Formathandboken/Lankning/index.html
        def hasBib880 = thing.remove(HAS_BIB880)

        if (!hasBib880) {
            return
        }
        
        Map bib880ByField = [:]

        asList(hasBib880).each { bib880 ->
            def linkField = bib880[PART_LIST][0][FIELDREF].split('-')
            def tag = linkField[0]
            def seqNum = linkField[1].take(2)
            if (seqNum != '00') {
                def entry = bib880ByField.computeIfAbsent(tag, s -> [])
                entry.add(['ref': '880-' + seqNum, 'bib880': bib880])
            }
        }

        Map bib880Map = [:]
        Map sameField = [:]

        bib880ByField.each { tag, data ->
            def marcJson = null
            try {
                marcJson = data.collect { bib880ToMarcJson(it.bib880) }
            } catch (Exception e) {
                return
            }
            
            def marc = [leader: "00887cam a2200277 a 4500", fields: marcJson]
            def converted = converter.runConvert(marc)

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
            def converted = bib880Map[ref]
            def mainEntity = DocumentUtil.getAtPath(converted, ['@graph', 1])
            if (sameField[ref]?.intersect(handled) || (mainEntity && mergeAltLanguage(mainEntity, thing))) {
                handled.add(ref)
                return new DocumentUtil.Remove()
            }
        }

        if (bib880Map) {
            FIELD_REFS.each {
                DocumentUtil.findKey(thing, it, handle880Ref)
            }
        }
    }

    void unmodify(Map record, Map thing) {
        if (!languageResources)
            return
        
        def byLangPaths = findByLangPaths(thing)
        def uniqueTLangs = findUniqueTLangs(thing, byLangPaths)

        // TODO: Can multiple transform rules be applied to the same record? What happens then with non-repeatable fields?
        uniqueTLangs.each { tLang ->
            unmodifyTLang(thing, tLang, byLangPaths, record)
        }

        byLangPaths.each { putRomanizedLiteralInNonByLang(thing, it as List) }
    }
    
    private def unmodifyTLang(def thing, def tLang, def byLangPaths, def record) {
        def copy = deepCopy(record)
        def thingCopy = copy.mainEntity

        byLangPaths.each { putOriginalLiteralInNonByLang(thingCopy, it as List, tLang) }
        def reverted = converter.runRevert(copy)
        
        /*
             We expect the order in reverted.fields to mirror the order in the "json data" for repeatable fields
             i.e. if e.g.
             thing.hasTitle[0] = {'@type': 'Title', 'mainTitle': 'Title 1'}
             thing.hasTitle[1] = {'@type': 'Title', 'mainTitle': 'Title 2'}
             then
             reverted.fields[i] = {'245': {'subfields': [{'a': 'Title 1'}]}}
             reverted.fields[i + 1] = {'245': {'subfields': [{'a': 'Title 2'}]}}

             If this can't be achieved we probably need to add some kind of reference to each
             (romanized) object *before* reverting in order to get the fieldrefs right.
             */
        
        List<Ref> fieldRefs = []
        
        List<String> paths = []
        DocumentUtil.findKey(thingCopy, '_revertedBy') { value, path ->
            paths.add(path.dropRight(1).join('-'))
            return DocumentUtil.NOP
        }

        List<String> nested = paths.findAll { p -> paths.any{ p.startsWith(it + '-')  } }
        paths = paths - nested
        
        DocumentUtil.findKey(thingCopy, '_revertedBy') { value, path ->
            if (path.dropRight(1).join('-') !in paths) {
                return 
            }
            
            def fieldMap = reverted.fields.find { it.containsKey(value) }
            if (!fieldMap) {
                return
            }
            def fieldNumber = value
            def field = fieldMap[fieldNumber]

            if (!field[SUBFIELDS].any { Map sf -> sf.values().any { it.startsWith(OG_MARK) }}) {
                return
            }
            
            def subFields = field[SUBFIELDS].collect {
                def subfield = it.keySet()[0]
                [(BIB880 + '-' + subfield): stripPrefix(it[subfield], OG_MARK)]
            }
            
            def hasBib880 = thing.computeIfAbsent(HAS_BIB880, s -> [])
            def ref = new Ref(
                    toField: fieldNumber,
                    occurenceNumber: hasBib880.size() + 1,
                    path: path.collect().dropRight(1)
            )

            def scriptCode = marcScript(tLang)

            def bib880 =
                    [
                            (TYPE)          : 'marc:Bib880',
                            (PART_LIST)     : [[(FIELDREF): ref.from880(scriptCode)]] + subFields,
                            (BIB880 + '-i1'): field[IND1],
                            (BIB880 + '-i2'): field[IND2]
                    ]

            hasBib880.add(bib880)
            reverted.fields.remove(fieldMap)

            fieldRefs.add(ref)
  
            return
        }

        fieldRefs.each { r ->
            def t = DocumentUtil.getAtPath(thing, r.path)
            t[r.propertyName()] = (asList(t[r.propertyName()]) << r.to880()).unique()
        }
    }
    
    private String marcScript(String tLang) {
        def script = languageResources.scripts[tLangCodes[tLang].fromLangScript]
        return MARC_SCRIPT_CODES[script?.code ?: '']
    }

    private static String stripPrefix(String s, String prefix) {
        s.startsWith(prefix) ? s.substring(prefix.length()) : s
    }
    
    @MapConstructor
    private class Ref {
        String toField
        int occurenceNumber
        List path
        
        String from880(String scriptCode) {
            "$toField-${String.format("%02d", occurenceNumber)}${scriptCode ?: ''}"
        }
        
        String to880() {
            "880-${String.format("%02d", occurenceNumber)}"
        }
        
        String propertyName() {
            switch (toField) {
                case '035': return BIB035_REF // TODO: also 'marc:hold035-fieldref'
                case '041': return BIB041_REF
                case '250': return BIB250_REF
                default: return FIELDREF
            }
        } 
    }
    
    boolean mergeAltLanguage(Map converted, Map thing) {
        // Since the 880s do not specify which language they are in, we assume that they are in the first work language
        def workLang = thing.instanceOf.subMap('language')
        languageResources.languageLinker.linkAll(workLang)
        def lang = asList(workLang.language).findResult { it[ID] } ?: 'https://id.kb.se/language/und'

        return addAltLang(thing, converted, lang)
    }

    boolean addAltLang(Map thing, Map converted, String lang) {
        if (!langIdToLangTag[lang] || !langToTLang[lang]) {
            return false
        }
        def nonByLangPaths = []
        DocumentUtil.findKey(converted, langAliases.keySet()) { value, path ->
            nonByLangPaths.add(path.collect())
            return
        }
        nonByLangPaths.each { path ->
            def containingObject = DocumentUtil.getAtPath(thing, path.dropRight(1))
            if (!containingObject) {
                //TODO: No romanized version seems to exists...
                return
            }

            asList(containingObject).each {
                def k = path.last()
                if (it[k]) {
                    def byLangProp = langAliases[k]
                    it[byLangProp] =
                            [
                                    (langIdToLangTag[lang]): DocumentUtil.getAtPath(converted, path),
                                    (langToTLang[lang])    : it[k]
                            ]
                }
                it.remove(k)
            }
        }

        return true
    }
    
    def putLiteralInNonByLang(Map thing, List byLangPath, Closure handler) {
        def key = byLangPath.last()
        def path = byLangPath.dropRight(1)
        Map parent = DocumentUtil.getAtPath(thing, path)

        def base = byLangToBase[key]
        if (base && parent[key] && !parent[base]) {
            handler(parent, key, base)
        }
        parent.remove(key)
    }

    def putRomanizedLiteralInNonByLang(Map thing, List byLangPath) {
        putLiteralInNonByLang(thing, byLangPath) { Map parent, String key, String base ->
            def langContainer = parent[key] as Map
            if (langContainer.size() == 1) {
                parent[base] = langContainer.values().first()
            } else {
                langContainer
                        .findResults { langTag, literal -> langTag in tLangCodes.keySet() ? literal : null }
                        .with(RomanizationStep::unpackSingle)
                        ?.with{ parent[base] = it }
            }
        }
    }

    static def unpackSingle(Collection l) {
        return l.size() == 1 ? l[0] : l
    }
    
    def putOriginalLiteralInNonByLang(Map thing, List byLangPath, String tLang) {
        putLiteralInNonByLang(thing, byLangPath) {  Map parent, String key, String base ->
            def romanized = parent[key].find { langTag, literal -> langTag == tLang }
            def original = parent[key].find { langTag, literal -> langTag == langIdToLangTag[tLangCodes[tLang].inLanguage] }?.value
            if (romanized && original) {
                parent[base] = original instanceof List
                        ? original.collect { OG_MARK + it }
                        : OG_MARK + original
            }
        }
    }

    def findByLangPaths(Map thing) {
        List paths = []

        DocumentUtil.findKey(thing, byLangToBase.keySet()) { value, path ->
            paths.add(path.collect())
            return
        }

        return paths
    }

    Set<String> findUniqueTLangs(Map thing, List<List> byLangPaths) {
        Set tLangs = []

        byLangPaths.each {
            DocumentUtil.getAtPath(thing, it).each { langTag, literal ->
                if (langTag in tLangCodes.keySet()) {
                    tLangs.add(langTag)
                }
            }
        }

        return tLangs
    }

    Map bib880ToMarcJson(Map bib880) {
        def parts = bib880[PART_LIST]
        def tag = parts[0][FIELDREF].split('-')[0]
        return [(tag): [
                (IND1)     : bib880["$BIB880-i1"],
                (IND2)     : bib880["$BIB880-i2"],
                (SUBFIELDS): parts[1..-1].collect {
                    def subfields = it.collect { key, value ->
                        [(key.replace('marc:bib880-', '')): value]
                    }
                    return subfields.size() == 1 ? subfields[0] : subfields
                }
        ]]
    }

    void init() {
        if (!languageResources) {
            return
        }
        
        this.tLangCodes = getTLangCodes(languageResources.transformedLanguageForms)
        this.langToTLang = tLangCodes.collectEntries { k, v -> [v.inLanguage, k] }
        this.langAliases = ld.langContainerAlias
        this.byLangToBase = langAliases.collectEntries { k, v -> [v, k] }
        this.langIdToLangTag = languageResources.languages
                .findAll { it.langTag }.collectEntries { [it[ID], it.langTag] }
    }

    static Map<String, Map> getTLangCodes(Map<String, Map> transformedLanguageForms) {
        String matchTag = "-${TARGET_SCRIPT}-t-"
        return transformedLanguageForms
                .values()
                .findAll{ ((String) it.langTag)?.contains(matchTag) }
                .collectEntries {
                    def data = [:]
                    if (it.inLanguage) {
                        data.inLanguage = it.inLanguage[ID]
                    }
                    if (it.fromLangScript) {
                        data.fromLangScript = it.fromLangScript[ID]
                    }
                    [it.langTag, data]
                }
    }
}

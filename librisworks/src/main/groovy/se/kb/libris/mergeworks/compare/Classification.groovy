package se.kb.libris.mergeworks.compare

class Classification extends StuffSet {
    private static def sabPrecedenceRules = loadSabPrecedenceRules()

    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b).findAll { it['code'] }) { c1, c2 ->
            String code1 = c1['code']
            String code2 = c2['code']
            if (!code1 || !code2) {
                return
            }

            if (isSab(c1) && isSab(c2)) {
                code1 = normalizeSabCode(code1)
                code2 = normalizeSabCode(code2)

                def mergedCode = tryMergeSabCodes(code1, code2)

                if (mergedCode) {
                    def result = [
                            '@type' : 'Classification',
                            'code'  : mergedCode,
                            inScheme: [
                                    '@type': 'ConceptScheme',
                                    'code' : 'kssb'
                            ]
                    ]
                    def version = maxSabVersion(c1, c2)
                    if (version) {
                        result['inScheme']['version'] = version
                    }
                    return result
                }
            } else if (isDewey(c1) && isDewey(c2)) {
                def code = code1.startsWith(code2.replace("/", ""))
                        ? code1
                        : (code2.startsWith(code1.replace("/", "")) ? code2 : null)
                if (code) {
                    Map result = [:]
                    result.putAll(c1)
                    result.putAll(c2)
                    result['code'] = code
                    result['editionEnumeration'] = maxDeweyEdition(c1, c2)
                    return result
                }
            }
        }
    }

    boolean isSab(Map c) {
        c['inScheme'] && c['inScheme']['code'] =~ 'kssb'
    }

    String maxSabVersion(c1, c2) {
        def v1 = c1['inScheme']['version'] ?: "-1"
        def v2 = c2['inScheme']['version'] ?: "-1"
        Integer.parseInt(v1) > Integer.parseInt(v2) ? v1 : v2
    }

    boolean isDewey(Map c) {
        c['@type'] == 'ClassificationDdc'
    }

    String maxDeweyEdition(c1, c2) {
        def v1 = c1['editionEnumeration']
        def v2 = c2['editionEnumeration']
        deweyEdition(v1) > deweyEdition(v2) ? v1 : v2
    }

    int deweyEdition(String edition) {
        Integer.parseInt((edition ?: "0").replaceAll("[^0-9]", ""))
    }

    static String normalizeSabCode(String sab) {
        sab.replaceFirst(~/^h/, 'H').with {
            it =~ /bf:|z/ ? it : it.replaceAll(~/\s+/, '')
        }
    }

    static String tryMergeSabCodes(String a, String b) {
        if (a == b) {
            return a
        }
        if (sabPrecedes(a, b)) {
            return a
        }
        if (sabPrecedes(b, a)) {
            return b
        }
        return null
    }

    static sabPrecedes(String a, String b) {
        def (equal, startsWith) = sabPrecedenceRules
        // Codes starting with Hcb or Hdab should never overwrite another code
        def overwriteExceptions = ~/^Hcb|^Hdab/
        def preferred = equal[b] ?: startsWith.find { b.startsWith(it.key) }?.value
        if (preferred && !(a =~ overwriteExceptions)) {
            if (preferred['equals'] && a in preferred['equals']) {
                return true
            }
            if (preferred['startsWith'] && preferred['startsWith'].any { a.startsWith(it) }) {
                return true
            }
        }
        return false
    }

    /**
     * Loads rules for how to merge SAB codes from file.
     * The code in the first column is preferred over the other codes in the same row.
     * The codes can contain wildcard characters '?' (anywhere in the string) or '*' (at the end)
     * The asterisk represents any sequence of characters (zero or more)
     * The question mark represents zero or one of the characters '6', '7' and '8'.
     * Examples:
     * Hcd* | Hcbd*
     *  --> Any code starting with Hcd is picked over any code starting with Hcbd
     * Hda.01?=c | Hda.01? | Hda=c
     *  --> Hda.01=c, Hda.016=c, Hda.017=c, Hda.018=c and Hda=c are all picked over over Hda.01, Hda.016, Hda.017, Hda.018 and Hda=c
     * Hcee.03 | Hce.03 | Hcee
     *  --> Hcee.03 is picked over Hce.03 and Hcee
     *
     * The rules are loaded into two different maps, 'equal' and 'startsWith'.
     * The top-level keys of these maps are the codes that can possibly be overwritten.
     *
     * In the 'equal' map we can directly look up a code (key) to see if there are preferred codes that should overwrite it,
     * while in the 'startsWith' map we check if the code starts with any of the keys. For example if the code is 'Hce'
     * and we have startsWith = ['He: [:], 'Hm': [:] 'Hc': [:]] we iterate over the entries until 'Hc' is found.
     *
     * The value is in turn also a Map containing the codes that are preferred over the code matching the key.
     * The map at this second level can have two keys, 'equals' and 'startsWith', and the values are sets of preferred codes.
     *
     * Example:
     * [
     *  'Hc.01': ['equals': ['Hc.01', 'Hc.016', 'Hc.017', 'Hc.018', 'Hcd.01', 'Hcd.016', 'Hcd.017', 'Hcd.018']],
     *  'Hce': ['startsWith': ['Hce']]
     * ]
     *
     * This means that any code starting with 'Hce' is preferred over just 'Hce' and any of 'Hc.01', 'Hc.016', 'Hc.017'...
     * is preferred over just 'Hc.01'.
     */
    static Tuple2<Map<String, Map>, Map<String, Map>> loadSabPrecedenceRules() {
        Map equal = [:]
        Map startsWith = [:]

        def questionMarkSubstitutes = ['6', '7', '8', '']

        Classification.class.getClassLoader()
                .getResourceAsStream('merge-works/sab-precedence-rules.tsv')
                .splitEachLine('\t') {
                    def preferred = it.first()
                    def preferredStartsWith = preferred.endsWith('*') ? preferred[0..<-1] : null
                    def preferredEquals = preferred.contains('?')
                            ? questionMarkSubstitutes.collect { preferred.replace('?', it) }
                            : (preferredStartsWith ? null : [preferred])

                    def addPreferred = { Map pref ->
                        if (preferredStartsWith) {
                            pref.computeIfAbsent('startsWith', f -> [] as Set).add(preferredStartsWith)
                        }
                        if (preferredEquals) {
                            pref.computeIfAbsent('equals', f -> [] as Set).addAll(preferredEquals)
                        }
                    }

                    def overwrite = it.drop(1)
                    overwrite.each { s ->
                        if (s.endsWith('*')) {
                            def leading = s[0..<-1]
                            startsWith.computeIfAbsent(leading, f -> [:]).with(addPreferred)
                        } else if (s.contains('?')) {
                            questionMarkSubstitutes.each {
                                def substituted = s.replace('?', it)
                                equal.computeIfAbsent(substituted, f -> [:]).with(addPreferred)
                            }
                        } else {
                            equal.computeIfAbsent(s, f -> [:]).with(addPreferred)
                        }
                    }
                }

        return new Tuple2(equal, startsWith)
    }
}
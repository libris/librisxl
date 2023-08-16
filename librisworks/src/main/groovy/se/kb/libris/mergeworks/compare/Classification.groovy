package se.kb.libris.mergeworks.compare

class Classification extends se.kb.libris.mergeworks.compare.StuffSet {
    // Terms that will be merged (values precede keys)
    private static def norm = [
            'uHc'                                                        : ['Hc,u'],
            'uHce'                                                       : ['Hce,u'],
            'Hc'                                                         : ['Hc.01', 'Hc.02', 'Hc.03'],
            'Hc,u'                                                       : ['Hcf', 'Hcg']
    ]

    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b).findAll { it['code'] }) { c1, c2 ->
            String code1 = c1['code']
            String code2 = c2['code']
            if (!code1 || !code2) {
                return
            }
            code1 = code1.replaceAll(/\s+/, "")
            code2 = code2.replaceAll(/\s+/, "")

            if (isSab(c1) && isSab(c2)) {
                def code = code1 == code2 || n(code2, code1)
                        ? code1
                        : (n(code1, code2) ? code2 : null)
                if (code) {
                    def result = [
                            '@type' : 'Classification',
                            'code'  : code1,
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
        c['inScheme'] && c['inScheme']['code'] == 'kssb'
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

    boolean n(a, b) {
        norm[a]?.any { it == b || n(it, b) }
    }
}
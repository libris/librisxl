package datatool.scripts.mergeworks.compare

class Classification extends StuffSet {
    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b)) { c1, c2 ->
            String code1 = c1['code']
            String code2 = c2['code']
            if (!code1 || !code2) {
                return
            }

            if (isSab(c1) && isSab(c2) && (code1.startsWith(code2) || code2.startsWith(code1))) {
                [
                        '@type' : 'Classification',
                        'code'  : code1.size() > code2.size() ? code1 : code2,
                        inScheme: [
                                '@type'  : 'ConceptScheme',
                                'code'   : 'kssb',
                                'version': maxSabVersion(c1, c2)
                        ]
                ]
            }
            else if (isDewey(c1) && isDewey(c2) && code1 == code2) {
                Map result = [:]
                result.putAll(c1)
                result.putAll(c2)
                result['editionEnumeration'] = maxDeweyEdition(c1, c2)
                result
            }
        }
    }

    boolean isSab(Map c) {
        c['inScheme'] && c['inScheme']['code'] == 'kssb'
    }

    String maxSabVersion(c1, c2) {
        def v1 = c1['inScheme']['version'] ?: "8"
        def v2 = c2['inScheme']['version'] ?: "8"
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
}
package datatool.scripts.mergeworks.compare

class Classification extends StuffSet {
    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b)) { c1, c2 ->
            if (isSab(c1) && isSab(c2) && (isPrefix(c1, c2) || isPrefix(c2, c1))) {
                [
                        '@type' : 'Classification',
                        'code'  : c1.size() > c2.size() ? c1 : c2,
                        inScheme: [
                                '@type'  : 'ConceptScheme',
                                'code'   : 'kssb',
                                'version': maxVersion(c1, c2)
                        ]
                ]
            }
        }
    }

    boolean isSab(Map c) {
        c['inScheme'] && c['inScheme']['code'] == 'kssb'
    }

    boolean isPrefix(c1, c2) {
        c1['code'] && c2['code'] && ((String) c2['code']).startsWith((String) c1['code'])
    }

    String maxVersion(c1, c2) {
        def v1 = c1['inScheme']['version'] ?: "8"
        def v2 = c2['inScheme']['version'] ?: "8"
        Integer.parseInt(v1) > Integer.parseInt(v2) ? v1 : v2
    }
}
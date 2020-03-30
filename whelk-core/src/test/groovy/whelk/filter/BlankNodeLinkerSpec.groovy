package whelk.filter

import spock.lang.Specification

class BlankNodeLinkerSpec extends Specification {
    BlankNodeLinker linker

    def setup() {
        linker = new BlankNodeLinker('Role', ['code', 'label', 'prefLabelByLang', 'altLabelByLang', 'hiddenLabel'])

        [
                [
                        '@id'          : 'http://id/aut',
                        code           : 'aut',
                        prefLabelByLang: [
                                'sv': 'Författare',
                                'en': 'Author',
                                'de': 'Verfasser',
                        ]
                ],
                [
                        '@id'            : 'http://id/trl',
                        code             : 'trl',
                        "prefLabelByLang": [
                                'de': 'Übersetzer',
                                'en': 'Translator',
                                'fi': 'Kääntäjä',
                                'is': 'Þýðandi',
                                'sv': 'Översättare'
                        ],
                        'hiddenLabel'    : [
                                'tõlkija',
                                'übers',
                                'traductor',
                                'traduction',
                                'translated by',
                                'tłumacz',
                                'tłumaczenie',
                                'tł'
                        ],
                        'altLabelByLang' : [
                                'de': 'Übersetzerin',
                                'en': 'translator'
                        ],
                ]
        ].each(linker.&addDefinition)

        linker.addSubstitutions([
                'auth': 'aut'
        ])
    }

    def "handles basic cases"() {
        expect:
        linker.linkAll(data, 'role') == change
        data == expected

        where:
        data                            | change | expected
        [role: [label: 'Författare']]   | true   | [role: ['@id': 'http://id/aut']]
        [role: [label: 'auth']]         | true   | [role: ['@id': 'http://id/aut']]
        [role: [label: 'tłumacz']]      | true   | [role: ['@id': 'http://id/trl']]
        [role: [label: 'Übersetzerin']] | true   | [role: ['@id': 'http://id/trl']]
    }
}

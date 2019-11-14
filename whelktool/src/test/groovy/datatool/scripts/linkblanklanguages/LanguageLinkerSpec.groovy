package datatool.scripts.linkblanklanguages

import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class LanguageLinkerSpec extends Specification {
    LanguageLinker linker

    def setup() {
        linker = new LanguageLinker()

        [
                [
                        '@id'          : 'http://id/swe',
                        code           : 'swe',
                        prefLabelByLang: [
                                'sv': 'Svenska',
                                'en': 'Swedish'
                        ]
                ],
                [
                        '@id'          : 'http://id/eng',
                        code           : 'eng',
                        prefLabelByLang: [
                                'sv': 'Engelska',
                                'en': 'English',
                                'de': 'Englisch'
                        ]
                ],
                [
                        '@id'          : 'http://id/nds',
                        code           : 'nds',
                        prefLabelByLang: [
                                'sv': 'Lågtyska'
                        ]
                ],
                [
                        '@id'          : 'http://id/heb',
                        code           : 'heb',
                        prefLabelByLang: [
                                'sv': 'Hebreiska',
                                'en': 'Hebrew'
                        ]
                ],
                [
                        '@id'          : 'http://id/fao',
                        code           : 'fao',
                        prefLabelByLang: [
                                'sv': 'Färöiska',
                                'en': 'Faroese'
                        ]
                ],
                [
                        '@id'          : 'http://id/lat',
                        code           : 'lat',
                        prefLabelByLang: [
                                'sv': 'Latin',
                                'en': 'Latin'
                        ]
                ],
                [
                        '@id'          : 'http://id/ara',
                        code           : 'ara',
                        prefLabelByLang: [
                                'sv': 'Arabiska',
                                'en': 'Arabic'
                        ]
                ],
                [
                        '@id'          : 'http://id/goh',
                        code           : 'goh',
                        prefLabelByLang: [
                                'sv': 'Fornhögtyska',
                                'en': 'German, Old High (ca.750-1050)'
                        ]
                ],
                [
                        '@id'          : 'http://id/gez',
                        code           : 'gez',
                        prefLabelByLang: [
                                "de": "Altäthiopisch",
                                "en": "Geez",
                                "fr": "guèze",
                                "sv": "Ge'ez"
                        ],
                        altLabelByLang : [
                                "sv": "Fornetiopiska"
                        ]

                ]
        ].each(linker.&addLanguageDefinition)

        linker.addSubstitutions([
                'tyska (fornhögtyska)': 'fornhögtyska'
        ])
    }

    def "handles basic cases"() {
        expect:
        linker.linkLanguages(data) == change
        data == expected

        where:
        data                                                                   | change | expected
        // all prefLabels are used
        [language: [label: 'Engelska']]                                        | true   | [language: ['@id': 'http://id/eng']]
        [language: [label: 'English']]                                         | true   | [language: ['@id': 'http://id/eng']]
        [language: [label: 'Englisch']]                                        | true   | [language: ['@id': 'http://id/eng']]

        // no duplicates
        [language: [[label: 'Engelska'], ['@id': 'http://id/eng']]]            | true   | [language: [['@id': 'http://id/eng']]]
        [language: [[code: 'Engelska'], ['@id': 'http://id/eng']]]             | true   | [language: [['@id': 'http://id/eng']]]
        [language: [[code: 'Engelska'], ['@id': 'http://id/eng', 'abc': 123]]] | true   | [language: [['@id': 'http://id/eng', 'abc': 123]]]

        // case insensitive
        [language: [label: 'engelska']]                                        | true   | [language: ['@id': 'http://id/eng']]
        [language: [code: 'ENG']]                                              | true   | [language: ['@id': 'http://id/eng']]

        // label in code field or code in label field
        [language: [code: 'Engelska']]                                         | true   | [language: ['@id': 'http://id/eng']]
        [language: [label: 'eng']]                                             | true   | [language: ['@id': 'http://id/eng']]

        // altLabel
        [language: [code: "Ge'ez"]]                                            | true   | [language: ['@id': 'http://id/gez']]
        [language: [label: 'fornetiopiska']]                                   | true   | [language: ['@id': 'http://id/gez']]

        // structure is preserved if possible - single object or list
        [language: [code: 'eng']]                                              | true   | [language: ['@id': 'http://id/eng']]
        [language: [[code: 'eng']]]                                            | true   | [language: [['@id': 'http://id/eng']]]

        // substitution map is used
        [language: [label: 'fornhögtyska']]                                    | true   | [language: ['@id': 'http://id/goh']]
        [language: [label: 'tyska (fornhögtyska)']]                            | true   | [language: ['@id': 'http://id/goh']]
        [language: [label: ['tyska (fornhögtyska)']]]                          | true   | [language: ['@id': 'http://id/goh']]
    }

    def "handles multiple language nodes"() {
        given:
        def data = [
                language: [label: 'Engelska'],
                a       : [
                        language: [label: 'Engelska'],
                        b       : [
                                [
                                        [language: [label: 'Engelska']],
                                        [language: [label: 'Engelska']],
                                        [c:
                                                 [language: [label: 'Engelska']]
                                        ]
                                ]
                        ]

                ],
                b       : [
                        language: [label: 'Engelska']
                ]
        ]

        expect:
        linker.linkLanguages(data) == true
        data == [
                language: ['@id': 'http://id/eng'],
                a       : [
                        language: ['@id': 'http://id/eng'],
                        b       : [
                                [
                                        [language: ['@id': 'http://id/eng']],
                                        [language: ['@id': 'http://id/eng']],
                                        [c:
                                                 [language: ['@id': 'http://id/eng']]
                                        ]
                                ]
                        ]

                ],
                b       : [
                        language: ['@id': 'http://id/eng']
                ]
        ]
    }

    def "handles concatenated language codes"() {
        expect:
        linker.linkLanguages(data) == change
        data == expected

        where:
        data                                  | change | expected
        [language: [code: 'engndsfao']]       | true   | [language: [['@id': 'http://id/eng'], ['@id': 'http://id/nds'], ['@id': 'http://id/fao']]]
        [language: [code: 'swe lat eng']]     | true   | [language: [['@id': 'http://id/swe'], ['@id': 'http://id/lat'], ['@id': 'http://id/eng']]]
        [language: [code: 'swe ; lat ; eng']] | true   | [language: [['@id': 'http://id/swe'], ['@id': 'http://id/lat'], ['@id': 'http://id/eng']]]

        // do nothing if not all codes can be mapped
        [language: [code: 'engndsfaoqqq']]    | false  | [language: [code: 'engndsfaoqqq']]
        [language: [code: 'swe qqq eng']]     | false  | [language: [code: 'swe qqq eng']]
    }

    def "handles weird language label lists"() {
        expect:
        linker.linkLanguages(data) == change
        data == expected

        where:
        data                                                 | change | expected
        [language: [label: ['Engelska', 'Hebreiska']]]       | true   | [language: [['@id': 'http://id/eng'], ['@id': 'http://id/heb']]]
        [language: [code: ['Engelska', 'Hebreiska', 'lat']]] | true   | [language: [['@id': 'http://id/eng'], ['@id': 'http://id/heb'], ['@id': 'http://id/lat']]]
        [language: [label: ['Engelska', 'Hebreiska', 'X']]]  | false  | [language: [label: ['Engelska', 'Hebreiska', 'X']]]
    }

    def "handles ambiguous labels"() {
        given:
        LanguageLinker linker = new LanguageLinker()
        [
                [
                        '@id'          : 'http://id/tog',
                        code           : 'tog',
                        prefLabelByLang: [
                                'sv': 'Tonga'
                        ],
                        commentByLang  : [
                                sv: 'Nyasa'
                        ]
                ],
                [
                        '@id'          : 'http://id/ton',
                        code           : 'ton',
                        prefLabelByLang: [
                                sv: 'Tonga'
                        ],
                        commentByLang  : [
                                sv: 'Tongaöarna'
                        ]
                ],
                [
                        '@id'          : 'http://id/gre',
                        code           : 'gre',
                        prefLabelByLang: [
                                'sv': 'Nygrekiska'
                        ]
                ],
                [
                        '@id'          : 'http://id/grc',
                        code           : 'grc',
                        prefLabelByLang: [
                                'sv': 'Grekiska, klassisk'
                        ]
                ],
                [
                        '@id'          : 'http://id/lat',
                        code           : 'lat',
                        prefLabelByLang: [
                                'sv': 'Latin',
                                'en': 'Latin'
                        ]
                ]
        ].each(linker.&addLanguageDefinition)
        linker.addMapping('grekiska', 'http://id/gre')
        linker.addMapping('grekiska', 'http://id/grc')

        expect:
        linker.linkLanguages(data) == change
        data == expected

        where:
        data                                                                | change | expected
        [language: [[label: 'tonga']]]                                      | false  | [language: [[label: 'tonga']]]
        [language: [[label: 'tonga'], ['@id': 'http://id/ton']]]            | true   | [language: [['@id': 'http://id/ton']]]
        [language: [[label: 'tonga'], ['@id': 'http://id/tog']]]            | true   | [language: [['@id': 'http://id/tog']]]

        [language: [[label: 'grekiska']]]                                   | false  | [language: [[label: 'grekiska']]]
        [language: [[label: 'grekiska'], ['@id': 'http://id/gre']]]         | true   | [language: [['@id': 'http://id/gre']]]
        [language: [[label: 'grekiska'], ['@id': 'http://id/grc']]]         | true   | [language: [['@id': 'http://id/grc']]]
    }


    def "removes non-existing sameAs links"() {
        expect:
        linker.linkLanguages(data) == change
        data == expected

        where:
        data                                                          | change | expected
        [language: [code: 'qqq', sameAs: [['@id': 'http://id/qqq']]]] | true   | [language: [code: 'qqq']]
        [language: [code: 'abc', sameAs: [['@id': 'http://id/fao']]]] | false  | [language: [code: 'abc', sameAs: [['@id': 'http://id/fao']]]]
    }

    def "handles language definitions with listed labels"() {
        given:
        LanguageLinker linker = new LanguageLinker()
        [
                [
                        '@id'          : 'http://id/swe',
                        code           : 'swe',
                        prefLabelByLang: [
                                'sv': ['Svenska', 'Svänska', 'Svånska']
                        ],
                        altLabelByLang : [
                                'sv': ['Svënskâ', 'Svénskã']
                        ]
                ]
        ].each(linker.&addLanguageDefinition)

        expect:
        linker.linkLanguages(data) == change
        data == expected

        where:
        data                           | change | expected
        [language: [label: 'Svenska']] | true   | [language: ['@id': 'http://id/swe']]
        [language: [label: 'Svänska']] | true   | [language: ['@id': 'http://id/swe']]
        [language: [label: 'Svånska']] | true   | [language: ['@id': 'http://id/swe']]
        [language: [label: 'Svënskâ']] | true   | [language: ['@id': 'http://id/swe']]
        [language: [label: 'Svénskã']] | true   | [language: ['@id': 'http://id/swe']]
    }

    def "handles noise"() {
        given:
        LanguageLinker linker = new LanguageLinker()
        [
                [
                        '@id'          : 'http://id/www',
                        code           : 'www',
                        prefLabelByLang: [
                                'sv': 'Ẽîïŷ'
                        ],
                        altLabelByLang : [
                                'sv': 'Ẽîïŷ99 (wwwiska)'
                        ]
                ]
        ].each(linker.&addLanguageDefinition)
        linker.linkLanguages(data)

        expect:
        data == expected

        where:
        data                                          | expected
        [language: [label: 'Ẽîïŷ']]                   | [language: ['@id': 'http://id/www']]
        [language: [label: 'Ẽîïŷ. ']]                 | [language: ['@id': 'http://id/www']]
        [language: [label: '   Ẽîïŷ      ']]          | [language: ['@id': 'http://id/www']]
        [language: [label: '--:Ẽîïŷ:--']]             | [language: ['@id': 'http://id/www']]
        [language: [label: '--:Ẽîïŷ99 (wwwiska):--']] | [language: ['@id': 'http://id/www']]
    }
}

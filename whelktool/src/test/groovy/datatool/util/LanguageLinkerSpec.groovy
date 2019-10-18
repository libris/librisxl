package datatool.util

import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class LanguageLinkerSpec extends Specification {
    LanguageMapper mapper

    def setup() {
        mapper = new LanguageMapper()

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
        ].each(mapper.&addLanguageDefinition)

        mapper.addSubstitutions([
                'tyska (fornhögtyska)': 'fornhögtyska'
        ])
    }

    def "handles basic cases"() {
        given:
        def c = new LanguageLinker(mapper)

        expect:
        c.linkLanguages(data) == change
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
    }

    def "handles concatenated language codes"() {
        given:
        def c = new LanguageLinker(mapper)

        expect:
        c.linkLanguages(data) == change
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

    def "handles enumerated language labels"() {
        given:
        def c = new LanguageLinker(mapper)

        expect:
        c.linkLanguages(data) == change
        data == expected

        where:
        data                                                                           | change | expected

        [language: [code: '. engelska & hebreiska']]                                   | true   | [language: [['@id': 'http://id/eng'], ['@id': 'http://id/heb']]]
        [language: [code: 'engelska, hebreiska, latin & arabiska']]                    | true   | [language: [['@id': 'http://id/eng'], ['@id': 'http://id/heb'], ['@id': 'http://id/lat'], ['@id': 'http://id/ara']]]
        [language: [code: '.tyska (fornhögtyska)., latin och svenska']]                | true   | [language: [['@id': 'http://id/goh'], ['@id': 'http://id/lat'], ['@id': 'http://id/swe']]]
        [language: [code: 'english and hebrew']]                                       | true   | [language: [['@id': 'http://id/eng'], ['@id': 'http://id/heb']]]

        // existing languages are not replaced
        [language: [['@id': 'http://id/heb'], [code: 'english and hebrew']]]           | true   | [language: [['@id': 'http://id/heb'], ['@id': 'http://id/eng']]]
        [language: [[code: 'english and hebrew'], ['@id': 'http://id/heb', abc: 123]]] | true   | [language: [['@id': 'http://id/eng'], ['@id': 'http://id/heb', abc: 123]]]

        // do nothing if not all labels can be mapped
        [language: [code: 'english and klingon']]                                      | false  | [language: [code: 'english and klingon']]
    }

    def "handles weird language label lists"() {
        given:
        def c = new LanguageLinker(mapper)

        expect:
        c.linkLanguages(data) == change
        data == expected

        where:
        data                                                 | change | expected
        [language: [label: ['Engelska', 'Hebreiska']]]       | true   | [language: [['@id': 'http://id/eng'], ['@id': 'http://id/heb']]]
        [language: [code: ['Engelska', 'Hebreiska', 'lat']]] | true   | [language: [['@id': 'http://id/eng'], ['@id': 'http://id/heb'], ['@id': 'http://id/lat']]]
        [language: [label: ['Engelska', 'Hebreiska', 'X']]]  | false  | [language: [label: ['Engelska', 'Hebreiska', 'X']]]
    }

    def "handles ambiguous labels"() {
        given:

        LanguageMapper mapper = new LanguageMapper()
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
        ].each(mapper.&addLanguageDefinition)
        mapper.addMapping('grekiska', 'http://id/gre')
        mapper.addMapping('grekiska', 'http://id/grc')
        def c = new LanguageLinker(mapper)

        expect:
        c.linkLanguages(data) == change
        data == expected

        where:
        data                                                                | change | expected
        [language: [[label: 'tonga']]]                                      | false  | [language: [[label: 'tonga']]]
        [language: [[label: 'tonga'], ['@id': 'http://id/ton']]]            | true   | [language: [['@id': 'http://id/ton']]]
        [language: [[label: 'tonga'], ['@id': 'http://id/tog']]]            | true   | [language: [['@id': 'http://id/tog']]]

        [language: [[label: 'grekiska']]]                                   | false  | [language: [[label: 'grekiska']]]
        [language: [[label: 'grekiska'], ['@id': 'http://id/gre']]]         | true   | [language: [['@id': 'http://id/gre']]]
        [language: [[label: 'grekiska'], ['@id': 'http://id/grc']]]         | true   | [language: [['@id': 'http://id/grc']]]

        [language: [[label: 'grekiska & latin']]]                           | false  | [language: [[label: 'grekiska & latin']]]
        [language: [[label: 'grekiska & latin'], ['@id': 'http://id/grc']]] | true   | [language: [['@id': 'http://id/lat'], ['@id': 'http://id/grc']]]
        [language: [[label: 'grekiska & latin'], ['@id': 'http://id/gre']]] | true   | [language: [['@id': 'http://id/lat'], ['@id': 'http://id/gre']]]
    }


    def "removes non-existing sameAs links"() {
        given:
        def c = new LanguageLinker(mapper)

        expect:
        c.linkLanguages(data) == change
        data == expected

        where:
        data                                               | change | expected
        [language: [code: 'qqq', sameAs: [['@id': 'http://id/qqq']]]] | true   | [language: [code: 'qqq']]
        [language: [code: 'abc', sameAs: [['@id': 'http://id/fao']]]] | false  | [language: [code: 'abc', sameAs: [['@id': 'http://id/fao']]]]
    }

    def "handles language definitions with listed labels"() {
        given:
        LanguageMapper mapper = new LanguageMapper()
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
        ].each(mapper.&addLanguageDefinition)
        def c = new LanguageLinker(mapper)

        expect:
        c.linkLanguages(data) == change
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
        LanguageMapper mapper = new LanguageMapper()
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
        ].each(mapper.&addLanguageDefinition)
        def c = new LanguageLinker(mapper)
        c.linkLanguages(data)

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

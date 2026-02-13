package whelk.util

import spock.lang.Specification
import whelk.JsonLd

import static whelk.component.ElasticSearch.DerivedLenses.CARD_ONLY
import static whelk.component.ElasticSearch.DerivedLenses.SEARCH_CARD_ONLY

// TODO test resourceStyle, propertyStyle, valueStyle
// TODO test card, full
// TODO test inverse properties

/**
 * These tests mostly use context, vocab and display that are similar to KBV.
 * This in order to make the tests a little less abstract and more readable.
 * Not sure if it makes it more confusing in the long run.
 */
class FresnelUtilSpec extends Specification {
    static final Map CONTEXT_DATA = [
            "@context": [
                    "@vocab"                       : "http://example.org/ns/",
                    "pfx"                          : "http://example.org/pfx/",
                    "labelByLang"                  : ["@id": "label", "@container": "@language"],
                    "prefLabelByLang"              : ["@id": "prefLabel", "@container": "@language"],
                    "mainTitleByLang"              : ["@id": "mainTitle", "@container": "@language"],
                    "subtitleByLang"               : ["@id": "subtitle", "@container": "@language"],
                    "partNameByLang"               : ["@id": "partName", "@container": "@language"],
                    "partNumberByLang"             : ["@id": "partNumber", "@container": "@language"],
                    "titleRemainderByLang"         : ["@id": "titleRemainder", "@container": "@language"],
                    "responsibilityStatementByLang": ["@id": "responsibilityStatement", "@container": "@language"],
                    "givenNameByLang"              : ["@id": "givenName", "@container": "@language"],
                    "familyNameByLang"             : ["@id": "familyName", "@container": "@language"],
                    "issuanceType"                 : ["@type": "@vocab"],
                    "langCodeBib"                  : ["@id": "code", "@type": "ISO639-2-B"],

            ]
    ]

    static final Map VOCAB_DATA = [
            "@graph": [
                    ["@id": "http://example.org/ns/Resource"],

                    ["@id"       : "http://example.org/ns/Identity",
                     "subClassOf": [["@id": "http://example.org/ns/Resource"]]],

                    ["@id"       : "http://example.org/ns/Thing",
                     "subClassOf": [["@id": "http://example.org/ns/Resource"]]],
                    ["@id"       : "http://example.org/ns/Work",
                     "subClassOf": [["@id": "http://example.org/ns/Resource"]]],
                    ["@id"       : "http://example.org/ns/StructuredValue",
                     "subClassOf": [["@id": "http://example.org/ns/Resource"]]],

                    ["@id"       : "http://example.org/ns/Identifier",
                     "subClassOf": [["@id": "http://example.org/ns/StructuredValue"]]],

                    ["@id"       : "http://example.org/ns/ISBN",
                     "@type"     : "Class",
                     "subClassOf": [["@id": "http://example.org/ns/Identifier"]]],
                    ["@id"       : "http://example.org/ns/ISNI",
                     "subClassOf": [["@id": "http://example.org/ns/Identifier"]]],
                    ["@id"       : "http://example.org/ns/ORCID",
                     "subClassOf": [["@id": "http://example.org/ns/Identifier"]]],
                    ["@id"        : "http://example.org/ns/MatrixNumber",
                     "@type"      : "Class",
                     "subClassOf" : [["@id": "http://example.org/ns/Identifier"]],
                     "labelByLang": ["en": "Audio matrix number", "sv": "Matrisnummer"],
                    ],

                    ["@id"       : "http://example.org/ns/Title",
                     "subClassOf": [["@id": "http://example.org/ns/StructuredValue"]]],
                    ["@id"       : "http://example.org/ns/KeyTitle",
                     "subClassOf": [["@id": "http://example.org/ns/Title"]]],
                    ["@id"       : "http://example.org/ns/VariantTitle",
                     "subClassOf": [["@id": "http://example.org/ns/Title"]]],

                    ["@id"       : "http://example.org/ns/TitlePart",
                     "subClassOf": [["@id": "http://example.org/ns/Resource"]]],

                    ["@id"       : "http://example.org/ns/Contribution",
                     "subClassOf": [["@id": "http://example.org/ns/Resource"]]],
                    ["@id"       : "http://example.org/ns/PrimaryContribution",
                     "subClassOf": [["@id": "http://example.org/ns/Contribution"]]],
                    ["@id"       : "http://example.org/ns/Language",
                     "subClassOf": [["@id": "http://example.org/ns/Resource"]]],

                    ["@id"       : "http://example.org/ns/Person",
                     "subClassOf": [["@id": "http://example.org/ns/Identity"]]],

                    ["@id"        : "http://example.org/ns/Serial",
                     "@type"      : "Class",
                     "subClassOf" : [["@id": "http://example.org/ns/Resource"]],
                     "labelByLang": ["en": "Serial", "sv": "Seriell resurs"],
                    ],

                    ["@id"       : "http://example.org/ns/Publication",
                     "subClassOf": ["@id": "http://example.org/ns/ProvisionActivity"]],
                    ["@id"       : "http://example.org/pfx/SpecialPublication",
                     "subClassOf": ["@id": "http://example.org/ns/Publication"]],

                    ["@id": "http://example.org/ns/label", "@type": ["@id": "DatatypeProperty"]],
                    ["@id"          : "http://example.org/ns/prefLabel",
                     "subPropertyOf": [["@id": "http://example.org/ns/label"]]],
                    ["@id"          : "http://example.org/ns/preferredLabel",
                     "subPropertyOf": ["@id": "http://example.org/ns/prefLabel"]],
                    ["@id"          : "http://example.org/ns/name",
                     "subPropertyOf": ["@id": "http://example.org/ns/label"]]
            ]
    ]

    static final Map DISPLAY_DATA = [
            "@context"  : [

            ],
            "lensGroups": [
                    "search-tokens": [
                            "@id"   : "search-tokens",
                            "@type" : "fresnel:Group",
                            "lenses": [
                                    "Work"  : [
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Work",
                                            "showProperties" : [
                                                    [
                                                            "alternateProperties": [
                                                                    [
                                                                            "@type" : "fresnel:fslselector",
                                                                            "@value": "hasTitle/Title/mainTitle"
                                                                    ],
                                                                    [
                                                                            "@type" : "fresnel:fslselector",
                                                                            "@value": "hasTitle/KeyTitle/mainTitle"
                                                                    ],
                                                                    [
                                                                            "@type" : "fresnel:fslselector",
                                                                            "@value": "hasTitle/*/mainTitle"
                                                                    ]
                                                            ]
                                                    ]
                                            ]
                                    ],
                                    "Person": [
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Person",
                                            "showProperties" : [
                                                    "givenName",
                                                    "familyName",
                                                    "name"
                                            ]
                                    ],
                                    "Title" : [
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Title",
                                            "fresnel:extends": ["@id": "Title-chips"],
                                            "showProperties" : ["fresnel:super"]
                                    ]
                            ]
                    ],
                    "tokens"       :
                            ["lenses": [
                                    "Contribution": [
                                            "@id"            : "Contribution-tokens",
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Contribution",
                                            "showProperties" : ["agent"]
                                    ],
                            ]],
                    "chips"        :
                            ["lenses": [
                                    "Contribution": [
                                            "@id"            : "Contribution-chips",
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Contribution",
                                            "showProperties" : ["agent", "role"]
                                    ],
                                    "Thing"       : ["showProperties": [
                                            "notation",
                                            "label",
                                            "note",
                                            "issuanceType",
                                            ["alternateProperties": [
                                                    "foo",
                                                    ["subPropertyOf": "bar", "range": "Bar"],
                                                    "title"
                                            ]]
                                    ]],
                                    "Person"      : [
                                            "@id"            : "Person-chips",
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Person",
                                            "showProperties" : ["familyName", "givenName", "name", "marc:numeration", "lifeSpan", "marc:titlesAndOtherWordsAssociatedWithAName", "fullerFormOfName"]
                                    ],
                                    "Title"       : [
                                            "@id"            : "Title-chips",
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Title",
                                            "showProperties" : [
                                                    "mainTitle",
                                                    "title",
                                                    "subtitle",
                                                    "titleRemainder",
                                                    "partNumber",
                                                    "partName",
                                                    [
                                                            "fresnel:property": "hasPart",
                                                            "fresnel:subLens": [
                                                                    "showProperties" : ["partNumber", "partName"]
                                                            ]
                                                    ]
                                            ]
                                    ],
                                    "TitlePart"   : [
                                            "@id"            : "TitlePart-chips",
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "TitlePart",
                                            "showProperties" : ["partNumber", "partName"]
                                    ],
                                    "Identifier"  : [
                                            "@id"            : "Identifier-chips",
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Identifier",
                                            "showProperties" : [
                                                    "rdf:type",
                                                    ["alternateProperties": ["value", "marc:hiddenValue"]],
                                                    "typeNote",
                                                    "hasNote"
                                            ]
                                    ],
                                    "Work"        : [
                                            "@id"            : "Work-chips",
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Work",
                                            "showProperties" : [
                                                    ["alternateProperties": [
                                                            ["subPropertyOf": "hasTitle", "range": "KeyTitle"],
                                                            ["subPropertyOf": "hasTitle", "range": "Title"],
                                                            "hasTitle"
                                                    ]],
                                                    "legalDate",
                                                    "language",
                                                    ["alternateProperties": [
                                                            ["subPropertyOf": "contribution", "range": "PrimaryContribution"]
                                                    ]],
                                                    "version",
                                                    "marc:arrangedStatementForMusic",
                                                    "originDate"
                                            ]
                                    ],
                            ]],
                    "cards"        :
                            ["lenses": [
                                    "Work": [
                                            "@id"            : "Work-cards",
                                            "@type"          : "fresnel:Lens",
                                            "classLensDomain": "Work",
                                            "showProperties" : [
                                                    ["alternateProperties": [
                                                            ["subPropertyOf": "hasTitle", "range": "KeyTitle"],
                                                            ["subPropertyOf": "hasTitle", "range": "Title"],
                                                            "hasTitle"
                                                    ]],
                                                    "legalDate",
                                                    "version",
                                                    "marc:arrangedStatementForMusic",
                                                    "originDate",
                                                    "contribution",
                                                    "language",
                                                    "translationOf",
                                                    "hasNotation",
                                                    "hasVariant",
                                                    "inCollection",
                                                    "genreForm",
                                                    "classification",
                                                    "subject",
                                                    "intendedAudience",
                                                    "contentType",
                                                    "dissertation",
                                                    "cartographicAttributes",
                                                    "isPartOf",
                                                    ["inverseOf": "instanceOf"]
                                            ]
                                    ],
                            ]]
            ],
            "formatters": [
                    'commaBeforeProperty-format': ['TODO': 'fullerFormOfName skrivs alltid med parenteser i libris nu, ska det vara så?', '@id': 'commaBeforeProperty-format', '@type': 'fresnel:Format', 'fresnel:propertyFormatDomain': ['lifeSpan', 'familyName', 'givenName', 'name', 'marc:numeration', 'lifeSpan', 'marc:titlesAndOtherWordsAssociatedWithAName', 'fullerFormOfName'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ', ', 'fresnel:contentFirst': '']],
                    'subtitle-format'           : ['@id': 'subtitle-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Title'], 'fresnel:propertyFormatDomain': ['subtitle', 'titleRemainder'], 'fresnel:propertyStyle': ['font-normal'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' : ', 'fresnel:contentFirst': '']],
                    'Title-qualifier-format'    : ['@id': 'Title-qualifier-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Title'], 'fresnel:propertyFormatDomain': ['qualifier'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' ', 'fresnel:contentFirst': '']],
                    'transliteration'           : ['@id': 'transliteration', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['_Transliterated'], 'fresnel:propertyFormatDomain': ['_transliterations'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' (', 'fresnel:contentFirst': '(', 'fresnel:contentAfter': ')', 'fresnel:contentLast': ')']],
                    'default-separators'        : ['@id': 'default-separators', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Resource'], 'fresnel:propertyFormatDomain': ['*'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' • ', 'fresnel:contentFirst': ''], 'fresnel:valueFormat': ['fresnel:contentBefore': ', ', 'fresnel:contentFirst': '']],
                    'Identifier-format'         : ['@id': 'Identifier-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Identifier'], 'fresnel:resourceStyle': ['ext-link', 'displayType()', 'uriToId()']],
                    'ISNI-digits-format'        : ['@id': 'ISNI-digits-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['ISNI', 'ORCID'], 'fresnel:propertyFormatDomain': ['value'], 'fresnel:valueStyle': ['isniGroupDigits()']],
                    'Identifier-value-format'   : ['@id': 'Identifier-value-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Identifier'], 'fresnel:propertyFormatDomain': ['value'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' ', 'fresnel:contentFirst': '']],
                    'hasTitle-format'           : ['@id': 'hasTitle-format', '@type': 'fresnel:Format', 'fresnel:propertyFormatDomain': ['hasTitle'], 'fresnel:valueFormat': ['fresnel:contentBefore': ' = ', 'fresnel:contentFirst': '']],
            ]
    ]

    // TODO JsonLd modifies DISPLAY_DATA every time it is loaded
    static final var ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

    def "format"() {
        given:
        var fresnel = new FresnelUtil(ld)
        var thing = [
                "@type"      : "Thing",
                "notation"   : "NOTATION",
                "labelByLang": ["sv": "etikett", "en": "label"],
                "title"      : [
                        "@type"          : "Title",
                        "mainTitleByLang": [
                                "el"                      : "Το νησι των θησαυρων",
                                "el-Latn-t-el-Grek-x0-btj": "To nisi ton thisavron"]],
                "note"       : ["NOTE 1", "NOTE 2"]
        ]

        when:
        var lensed = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)
        var formatted = fresnel.format(lensed, new FresnelUtil.LangCode('sv'))

        then:
        formatted.asString() == "NOTATION • etikett • NOTE 1, NOTE 2 • Το νησι των θησαυρων ’To nisi ton thisavron’"

        when:
        var chip = fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip == [
                "@type"      : "Thing",
                "notation"   : "NOTATION",
                "labelByLang": [
                        "en": "label"
                ],
                "note"       : ["NOTE 1", "NOTE 2"],
                "title"      : [
                        "@type"          : "Title",
                        "mainTitleByLang": [
                                "el-Latn-t-el-Grek-x0-btj": "To nisi ton thisavron",
                                "el"                      : "Το νησι των θησαυρων"
                        ],
                        "_str"           : ["To nisi ton thisavron", "Το νησι των θησαυρων"]
                ]
        ]
    }

    def "Heliogabalus"() {
        given:
        var fresnel = new FresnelUtil(ld)
        var thing = [
                '@id'                                        : 'https://libris-qa.kb.se/khw03jc347kgv4w#it',
                'name'                                       : 'Heliogabalus',
                '@type'                                      : 'Person',
                'image'                                      : [['@id': 'https://libris.kb.se/dataset/images/8d35730bbea96c833b7a54124eeecd9d.full.jpg']],
                'sameAs'                                     : [['@id': 'http://libris.kb.se/resource/auth/281943']],
                'lifeSpan'                                   : '203-222',
                'exactMatch'                                 : [['@id': 'http://www.wikidata.org/entity/Q1762']],
                'hasVariant'                                 : [['name': 'Elagabalus', '@type': 'Person', 'lifeSpan': '203-222', 'marc:titlesAndOtherWordsAssociatedWithAName': ['romersk kejsare']], ['name': 'Marcus Aurelius Antoninus', '@type': 'Person', 'lifeSpan': '203-222', 'marc:titlesAndOtherWordsAssociatedWithAName': ['romersk kejsare']], ['@type': 'Person', 'lifeSpan': '203-222', 'givenName': 'Varius Avitus', 'familyName': 'Bassianus', 'marc:titlesAndOtherWordsAssociatedWithAName': ['romersk kejsare']], ['name': 'Héliogabale', '@type': 'Person', 'lifeSpan': '203-222', 'marc:titlesAndOtherWordsAssociatedWithAName': ['romersk kejsare']], ['name': 'Elagabale', '@type': 'Person', 'lifeSpan': '203-222', 'marc:titlesAndOtherWordsAssociatedWithAName': ['romersk kejsare']], ['name': 'El Gabal', '@type': 'Person', 'lifeSpan': '203-222', 'marc:titlesAndOtherWordsAssociatedWithAName': ['romersk kejsare']], ['name': 'Elagabalo', '@type': 'Person', 'lifeSpan': '203-222', 'marc:titlesAndOtherWordsAssociatedWithAName': ['romersk kejsare']]],
                'description'                                : ['Romersk kejsare från 218; likviderad'],
                'nationality'                                : [['@id': 'https://id.kb.se/nationality/xx']],
                'identifiedBy'                               : [['@type': 'VIAF', 'value': '24583475'], ['@type': 'ISNI', 'value': '0000000103407488']],
                'marc:titlesAndOtherWordsAssociatedWithAName': ['romersk kejsare']
        ]

        when:
        var formatted = fresnel.format(fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN), new FresnelUtil.LangCode('sv'))

        then:
        formatted.asString() == "Heliogabalus, 203-222, romersk kejsare"

        when:
        var chip = fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip == [
                "@id"                                        : "https://libris-qa.kb.se/khw03jc347kgv4w#it",
                "@type"                                      : "Person",
                'sameAs'                                     : [['@id': 'http://libris.kb.se/resource/auth/281943']],
                "name"                                       : "Heliogabalus",
                "lifeSpan"                                   : "203-222",
                "marc:titlesAndOtherWordsAssociatedWithAName": "romersk kejsare",
                "_str"                                       : "Heliogabalus"
        ]
    }

    def "complex title"() {
        given:
        var thing = [
                '@type'    : 'Title',
                'mainTitle': 'Teater-biblioteket',
                'hasPart'  : [
                        [
                                '@type'     : 'TitlePart',
                                'partName'  : ['En friare i lifsfara : skämt med sång i en akt'],
                                'partNumber': ['N:o 15']
                        ]
                ]
        ]

        var fresnel = new FresnelUtil(ld)

        when:
        var formatted = fresnel.format(fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN), new FresnelUtil.LangCode('sv'))

        then:
        formatted.asString() == "Teater-biblioteket • N:o 15 • En friare i lifsfara : skämt med sång i en akt"

        when:
        var chip = fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip == [
                "@type"    : "Title",
                "mainTitle": "Teater-biblioteket",
                "hasPart"  : [
                        "@type"     : "TitlePart",
                        "partNumber": "N:o 15",
                        "partName"  : "En friare i lifsfara : skämt med sång i en akt"
                ],
                "_str"     : "Teater-biblioteket N:o 15 En friare i lifsfara : skämt med sång i en akt"
        ]
    }

    def "group transliteration for structured value"() {
        given:
        var fresnel = new FresnelUtil(ld)
        var thing = [
                "@type"          : "Title",
                "mainTitleByLang": [
                        "grc"                            : "Νεφέλαι",
                        "grc-Latn-t-grc-Grek-x0-skr-1980": "Nephelai"],
                "subtitleByLang" : [
                        "grc"                            : "Λυσιστράτη",
                        "grc-Latn-t-grc-Grek-x0-skr-1980": "Lysistratē"]
        ]

        when:
        var formatted = fresnel.format(fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN), new FresnelUtil.LangCode('sv'))

        then:
        formatted.asString() == "Νεφέλαι : Λυσιστράτη ’Nephelai : Lysistratē’"

        when:
        var chip = fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip == [
                "@type"          : "Title",
                "mainTitleByLang": [
                        "grc-Latn-t-grc-Grek-x0-skr-1980": "Nephelai",
                        "grc"                            : "Νεφέλαι"
                ],
                "subtitleByLang" : [
                        "grc-Latn-t-grc-Grek-x0-skr-1980": "Lysistratē",
                        "grc"                            : "Λυσιστράτη"
                ],
                "_str"           : ["Nephelai Lysistratē", "Νεφέλαι Λυσιστράτη"]
        ]
    }

    def "Handle RTL original script"() {
        given:
        var fresnel = new FresnelUtil(ld)
        var thing = [
                "@type"          : "Title",
                "mainTitleByLang": [
                        "ira"                : "تالشی زَوُن",
                        "ira-Latn-t-ira-Arab": "talysj"],
        ]

        when:
        var formatted = fresnel.format(fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN), new FresnelUtil.LangCode('sv'))

        then:
        formatted.asString() == "⁧تالشی زَوُن⁩ ’talysj’"

        when:
        var chip = fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip == [
                "@type"          : "Title",
                "mainTitleByLang": [
                        "ira-Latn-t-ira-Arab": "talysj",
                        "ira"                : "تالشی زَوُن"],
                "_str"           : ["talysj", "تالشی زَوُن"]
        ]
    }

    def "showProperties rdf:type"() {
        given:
        var fresnel = new FresnelUtil(ld)

        expect:
        fresnel.format(fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN), new FresnelUtil.LangCode(locale)).asString() == result
        fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN) == thing

        where:
        thing                                                  | locale | result
        ['@type': 'ISBN', 'value': '9789174156065']            | 'sv'   | "ISBN 9789174156065"
        ['@type': 'MatrixNumber', 'value': 'EMI 724352190508'] | 'sv'   | "Matrisnummer EMI 724352190508"
        ['@type': 'MatrixNumber', 'value': 'EMI 724352190508'] | 'en'   | "Audio matrix number EMI 724352190508"
    }

    def "@type @vocab terms"() {
        given:
        var fresnel = new FresnelUtil(ld)

        expect:
        fresnel.format(fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN), new FresnelUtil.LangCode(locale)).asString() == result
        fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN) == thing

        where:
        thing                                        | locale | result
        ['@type': 'Thing', 'issuanceType': 'Serial'] | 'sv'   | 'Seriell resurs'
        ['@type': 'Thing', 'issuanceType': 'Serial'] | 'en'   | 'Serial'
    }


    static var kt = ['@type': 'KeyTitle', 'mainTitle': 'key title']
    static var kt2 = ['@type': 'KeyTitle', 'mainTitle': 'key title 2']
    static var tt = ['@type': 'Title', 'mainTitle': 'title']
    static var vt = ['@type': 'VariantTitle', 'mainTitle': 'variant title']

    def "alternateProperties + range restriction"() {
        given:
        var fresnel = new FresnelUtil(ld)

        expect:
        fresnel.format(fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN), new FresnelUtil.LangCode(locale)).asString() == result

        where:
        thing                                        | locale | result
        ['@type': 'Work', 'hasTitle': [vt, tt, kt]]  | 'sv'   | 'key title'
        ['@type': 'Work', 'hasTitle': [tt, kt, vt]]  | 'sv'   | 'key title'
        ['@type': 'Work', 'hasTitle': [kt, vt, tt]]  | 'sv'   | 'key title'
        ['@type': 'Work', 'hasTitle': [vt, tt]]      | 'sv'   | 'title'
        ['@type': 'Work', 'hasTitle': [tt, vt]]      | 'sv'   | 'title'
        ['@type': 'Work', 'hasTitle': [vt]]          | 'sv'   | 'variant title'
        ['@type': 'Work', 'hasTitle': [tt, kt, kt2]] | 'sv'   | 'key title = key title 2'
        ['@type': 'Work', 'hasTitle': [tt, kt2, kt]] | 'sv'   | 'key title 2 = key title'
    }

    def "Chip of alternate properties"() {
        given:
        var fresnel = new FresnelUtil(ld)
        var thing = ['@type': 'Work', 'hasTitle': [tt, kt, kt2, vt]]
        var chip = fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        expect:
        chip == [
                "@type"   : "Work",
                "hasTitle": [[
                                     "@type"    : "KeyTitle",
                                     "mainTitle": "key title",
                                     "_str"     : "key title"
                             ], [
                                     "@type"    : "KeyTitle",
                                     "mainTitle": "key title 2",
                                     "_str"     : "key title 2"
                             ], [
                                     "@type"    : "Title",
                                     "mainTitle": "title",
                                     "_str"     : "title"
                             ], [
                                     "@type"    : "VariantTitle",
                                     "mainTitle": "variant title",
                                     "_str"     : "variant title"
                             ]],
                "_str"    : "title"
        ]
    }

    def "isniGroupDigits"() {
        given:
        var thing = [
                '@type': 'ISNI',
                'value': '0000000103407488'
        ]

        var fresnel = new FresnelUtil(ld)
        var result = fresnel.format(fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN), new FresnelUtil.LangCode('sv'))

        expect:
        result.asString() == "ISNI 0000 0001 0340 7488"
    }

    def "chips vs tokens"() {
        given:
        var thing = [
                '@type'       : 'Work',
                'hasTitle'    : [
                        ['@type': 'Title', 'mainTitle': 'Titel'],
                        ['@type': 'VariantTitle', 'mainTitle': 'variant title']
                ],
                'language'    : [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ],
                'contribution': [
                        ['@type': 'Contribution', 'role': 'translator', 'agent': ['@type': 'Person', 'name': 'Överzet']],
                        ['@type': 'PrimaryContribution', 'role': 'author', 'agent': ['@type': 'Person', 'givenName': 'Namn', 'familyName': 'Namnsson', 'lifeSpan': "1972-"]],
                ]
        ]

        var fresnel = new FresnelUtil(ld)

        when:
        var formatted = fresnel.format(fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN), new FresnelUtil.LangCode('sv'))

        then:
        formatted.asString() == "Titel • Svenska • Namnsson, Namn, 1972-"

        when:
        var chip = fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip == [
                "@type"       : "Work",
                "hasTitle"    : [[
                                         "@type"    : "Title",
                                         "mainTitle": "Titel",
                                         "_str"     : "Titel"
                                 ], [
                                         "@type"    : "VariantTitle",
                                         "mainTitle": "variant title",
                                         "_str"     : "variant title"
                                 ]],
                "language"    : [
                        "@type"      : "Language",
                        "labelByLang": [
                                "en": "Swedish"
                        ]
                ],
                "contribution": [
                        "@type": "PrimaryContribution",
                        "agent": [
                                "@type"     : "Person",
                                "familyName": "Namnsson",
                                "givenName" : "Namn",
                                "lifeSpan"  : "1972-",
                                "_str"      : "Namn Namnsson"
                        ]
                ],
                "_str"        : "Titel"
        ]
    }

    def "derived lens"() {
        given:
        var thing = [
                '@type'       : 'Work',
                'hasTitle'    : [
                        ['@type': 'Title', 'mainTitle': 'Titel'],
                        ['@type': 'VariantTitle', 'mainTitle': 'variant title']
                ],
                'language'    : [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ],
                'subject'     : [
                        ['@type': 'Topic', 'prefLabel': "Hästar"],
                ],
                'contribution': [
                        ['@type': 'Contribution', 'role': 'translator', 'agent': ['@type': 'Person', 'name': 'Överzet']],
                        ['@type': 'PrimaryContribution', 'role': 'author', 'agent': ['@type': 'Person', 'givenName': 'Namn', 'familyName': 'Namnsson', 'lifeSpan': "1972-"]],
                ]
        ]

        var fresnel = new FresnelUtil(ld)
        var cardOnly = new FresnelUtil.Lens(
                FresnelUtil.CARD_CHAIN,
                FresnelUtil.Lenses.TOKEN,
                List.of(FresnelUtil.CHIP_CHAIN)
        )

        when:
        var lensed = fresnel.applyLens(thing, cardOnly)

        then:
        lensed.asString() == "Överzet Namnsson Namn 1972- Hästar"

        when:
        var lensedThing = fresnel.getLensedThing(thing, cardOnly)

        then:
        lensedThing == [
                "@type"       : "Work",
                "contribution": [[
                                         "@type": "Contribution",
                                         "agent": [
                                                 "@type": "Person",
                                                 "name" : "Överzet",
                                                 "_str" : "Överzet"
                                         ]
                                 ], [
                                         "@type": "PrimaryContribution",
                                         "agent": [
                                                 "@type"     : "Person",
                                                 "familyName": "Namnsson",
                                                 "givenName" : "Namn",
                                                 "lifeSpan"  : "1972-",
                                                 "_str"      : "Namn Namnsson"
                                         ]
                                 ]],
                "subject"     : [
                        "@type"    : "Topic",
                        "prefLabel": "Hästar"
                ]
        ]
    }

    def "take all alternate"() {
        given:
        var thing = [
                '@type'       : 'Work',
                'hasTitle'    : [
                        ['@type': 'Title', 'mainTitle': 'Titel'],
                        ['@type': 'VariantTitle', 'mainTitle': 'variant title']
                ],
                'language'    : [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ],
                'subject'     : [
                        ['@type': 'Topic', 'prefLabel': "Hästar"],
                ],
                'contribution': [
                        ['@type': 'Contribution', 'role': 'translator', 'agent': ['@type': 'Person', 'name': 'Överzet']],
                        ['@type': 'PrimaryContribution', 'role': 'author', 'agent': ['@type': 'Person', 'givenName': 'Namn', 'familyName': 'Namnsson', 'lifeSpan': "1972-"]],
                ]
        ]
        var fresnel = new FresnelUtil(ld)

        when:
        var lensed = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CARD_TO_CHIP_TO_TOKEN, [FresnelUtil.Options.TAKE_ALL_ALTERNATE])

        then:
        lensed.asString() == "Titel Titel variant title Överzet translator Namnsson Namn 1972- author Swedish Svenska Hästar"

        when:
        var lensedThing = fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CARD_TO_CHIP_TO_TOKEN)

        then:
        lensedThing == [
                "@type"       : "Work",
                "hasTitle"    : [[
                                         "@type"    : "Title",
                                         "mainTitle": "Titel",
                                         "_str"     : "Titel"
                                 ], [
                                         "@type"    : "VariantTitle",
                                         "mainTitle": "variant title",
                                         "_str"     : "variant title"
                                 ]],
                "contribution": [[
                                         "@type": "Contribution",
                                         "agent": [
                                                 "@type": "Person",
                                                 "name" : "Överzet",
                                                 "_str" : "Överzet"
                                         ],
                                         "role" : "translator"
                                 ], [
                                         "@type": "PrimaryContribution",
                                         "agent": [
                                                 "@type"     : "Person",
                                                 "familyName": "Namnsson",
                                                 "givenName" : "Namn",
                                                 "lifeSpan"  : "1972-",
                                                 "_str"      : "Namn Namnsson"
                                         ],
                                         "role" : "author"
                                 ]],
                "language"    : [
                        "@type"      : "Language",
                        "labelByLang": [
                                "en": "Swedish"
                        ]
                ],
                "subject"     : [
                        "@type"    : "Topic",
                        "prefLabel": "Hästar"
                ],
                "_str"        : "Titel"
        ]
    }

    static var tt2 = ['@type': 'Title', 'mainTitle': 'Titel', 'subtitle': 'undertitel']
    static var vt2 = ['@type': 'VariantTitle', 'mainTitleByLang': ['en': 'variant title']]
    static var mt = ['@type': 'Title', 'mainTitle': 'Titel']
    static var l = ['@type': 'Language', 'labelByLang': ['en': 'Swedish']]
    static var l2 = ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish']]

    def "Handle FSL path in showProperties"() {
        // https://www.w3.org/2005/04/fresnel-info/fsl/
        given:
        Map displayData = [
                "@context"  : [

                ],
                "lensGroups": [
                        "chips":
                                ["lenses": [
                                        "Work" : [
                                                "@id"            : "Work-chips",
                                                "@type"          : "fresnel:Lens",
                                                "classLensDomain": "Work",
                                                "showProperties" : [
                                                        [
                                                                "@type" : "fresnel:fslselector",
                                                                "@value": fslPath
                                                        ],
                                                        "language"
                                                ]
                                        ],
                                        "Title": [
                                                "@id"            : "Title-chips",
                                                "@type"          : "fresnel:Lens",
                                                "classLensDomain": "Title",
                                                "showProperties" : ["mainTitle", "title", "subtitle", "titleRemainder", "partNumber", "partName", "hasPart"]
                                        ],
                                ]]
                ],
        ]
        var thing = [
                '@type'   : 'Work',
                'hasTitle': [
                        ['@type': 'Title', 'mainTitle': 'Titel', 'subtitle': 'undertitel'],
                        ['@type': 'VariantTitle', 'mainTitleByLang': ['sv': 'varianttitel', 'en': 'variant title']]
                ],
                'language': [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ],
                'subject' : [
                        ['@type': 'Topic', 'prefLabel': "Hästar"],
                ],
                '@reverse': [
                        'instanceOf': [
                                '@type'                  : 'Instance',
                                'responsibilityStatement': 'av En Författare',
                                'meta'                   : [
                                        '@type'        : 'Record',
                                        'controlNumber': 'dz7666s5bdksvtls'
                                ]
                        ]
                ]
        ]
        var fresnel = new FresnelUtil(new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA))

        var lensed = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)
        var lensedThing = fresnel.getLensedThing(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        expect:
        lensed.asString() == str
        lensedThing == asThing

        where:
        fslPath                                    | str                                                           | asThing
        "hasTitle"                                 | "Titel undertitel varianttitel variant title Swedish Svenska" | ['@type': 'Work', 'hasTitle': [tt2, vt2], 'language': l]
        "hasTitle/Title/mainTitle"                 | "Titel Swedish Svenska"                                       | ['@type': 'Work', 'hasTitle': mt, 'language': l]
        "hasTitle[Title]"                          | "Titel undertitel Swedish Svenska"                            | ['@type': 'Work', 'hasTitle': tt2, 'language': l]
        "hasTitle[^Title]"                         | "Titel undertitel varianttitel variant title Swedish Svenska" | ['@type': 'Work', 'hasTitle': [tt2, vt2], 'language': l]
        "hasTitle/VariantTitle/mainTitle"          | "varianttitel variant title Swedish Svenska"                  | ['@type': 'Work', 'hasTitle': vt2, 'language': l]
        "hasTitle/*/mainTitle"                     | "Titel varianttitel variant title Swedish Svenska"            | ['@type': 'Work', 'hasTitle': [mt, vt2], 'language': l]
        "language/Language/code"                   | "sv Swedish Svenska"                                          | ['@type': 'Work', 'language': l2]
        "language/Language/mainTitle"              | "Swedish Svenska"                                             | ['@type': 'Work', 'language': l]
        "language/Title/code"                      | "Swedish Svenska"                                             | ['@type': 'Work', 'language': l]
        'in::instanceOf/*/responsibilityStatement' | 'av En Författare Swedish Svenska'                            | ['@type': 'Work', 'language': l, '@reverse': ['instanceOf': ['@type': 'Instance', 'responsibilityStatement': 'av En Författare']]]
        'in::instanceOf/*/meta/*/controlNumber'    | 'dz7666s5bdksvtls Swedish Svenska'                            | ['@type': 'Work', 'language': l, '@reverse': ['instanceOf': ['@type': 'Instance', 'meta': ['@type': 'Record', 'controlNumber': 'dz7666s5bdksvtls']]]]
    }

    def "Handle FSL paths as alternateProperties"() {
        given:
        List alternateProperties = fslPaths.collect { ["@type": "fresnel:fslselector", "@value": it] }
        Map displayData = [
                "@context"  : [

                ],
                "lensGroups": [
                        "chips":
                                ["lenses": [
                                        "Thing": [
                                                "@id"            : "Thing-chips",
                                                "@type"          : "fresnel:Lens",
                                                "classLensDomain": "Thing",
                                                "showProperties" : [
                                                        [
                                                                "alternateProperties": alternateProperties
                                                        ]
                                                ]
                                        ]
                                ]]
                ],
        ]
        var thing = [
                '@type'   : 'Thing',
                'hasTitle': [
                        ['@type': 'Title', 'mainTitle': 'Titel'],
                        ['@type': 'VariantTitle', 'mainTitle': 'variant title']
                ]
        ]
        var fresnel = new FresnelUtil(new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA))

        var takeFirst = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)
        var takeAllAlternate = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN, [FresnelUtil.Options.TAKE_ALL_ALTERNATE])

        expect:
        takeFirst.asString() == takeFirstRes
        takeAllAlternate.asString() == takeAllAlternateRes

        where:
        fslPaths                                                           | takeFirstRes          | takeAllAlternateRes
        ["hasTitle/Title/mainTitle", "hasTitle/VariantTitle/mainTitle"]    | "Titel"               | "Titel variant title"
        ["hasTitle/VariantTitle/mainTitle", "hasTitle/Title/mainTitle"]    | "variant title"       | "variant title Titel"
        ["hasTitle/KeyTitle/mainTitle", "hasTitle/VariantTitle/mainTitle"] | "variant title"       | "variant title"
        ["hasTitle/KeyTitle/mainTitle", "hasTitle/*/mainTitle"]            | "Titel variant title" | "Titel variant title"
    }

    def "Split up strings by language"() {
        given:
        var thing = [
                '@type'   : 'Work',
                'hasTitle': title,
                'language': [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ]
        ]
        var fresnel = new FresnelUtil(ld)
        var chip = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        expect:
        chip.byLang() == res

        where:
        title                                                                                                      | res
        [['@type': 'Title', 'mainTitle': 'Titel'], ['@type': 'VariantTitle', 'mainTitle': 'variant title']]        | ['sv': 'Titel Svenska', 'en': 'Titel Swedish']
        [['@type': 'VariantTitle', 'mainTitle': 'variant title']]                                                  | ['sv': 'variant title Svenska', 'en': 'variant title Swedish']
        [['@type': 'Title', 'mainTitleByLang': ['sv': 'Titel', 'en': 'Title']]]                                    | ['sv': 'Titel Svenska', 'en': 'Title Swedish']
        [['@type': 'Title', 'mainTitleByLang': ['sv': 'Titel', 'en': 'Title'], 'subtitle': 'subtitle']]            | ['sv': 'Titel subtitle Svenska', 'en': 'Title subtitle Swedish']
        [['@type': 'Title', 'mainTitleByLang': ["grc": "Νεφέλαι", "grc-Latn-t-grc-Grek-x0-skr-1980": "Nephelai"]]] | ['sv': 'Nephelai Νεφέλαι Svenska', 'en': 'Nephelai Νεφέλαι Swedish']
    }

    def "Split up strings by script language"() {
        given:
        var fresnel = new FresnelUtil(ld)
        var thing
        var chip

        when:
        thing = [
                '@type'   : 'Work',
                'hasTitle': [['@type': 'Title', 'mainTitle': 'Titel'], ['@type': 'VariantTitle', 'mainTitle': 'variant title']],
                'language': [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ]
        ]
        chip = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip.byScript() == [:]

        when:
        thing = [
                '@type'   : 'Work',
                'hasTitle': [['@type': 'Title', 'mainTitleByLang': ["grc": "Νεφέλαι", "grc-Latn-t-grc-Grek-x0-skr-1980": "Nephelai"]]],
                'language': [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ]
        ]
        chip = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip.byScript() == ['grc': 'Νεφέλαι Swedish Svenska', 'grc-Latn-t-grc-Grek-x0-skr-1980': 'Nephelai Swedish Svenska']

        when:
        thing = [
                '@type'   : 'Work',
                'hasTitle': [['@type': 'Title', 'mainTitleByLang': ["grc": "Νεφέλαι", "grc-Latn-t-grc-Grek-x0-skr-1980": "Nephelai"], 'subtitle': 'subtitle']],
                'language': [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ]
        ]
        chip = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip.byScript() == ['grc': 'Νεφέλαι subtitle Swedish Svenska', 'grc-Latn-t-grc-Grek-x0-skr-1980': 'Nephelai subtitle Swedish Svenska']

        when:
        thing = [
                '@type'   : 'Work',
                'hasTitle': [['@type'          : 'Title',
                              'mainTitleByLang': ["grc": "Νεφέλαι", "grc-Latn-t-grc-Grek-x0-skr-1980": "Nephelai"],
                              'subtitleByLang' : ["grc": "Λυσιστράτη", "grc-Latn-t-grc-Grek-x0-skr-1980": "Lysistratē"]]],
                'language': [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ]
        ]
        chip = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip.byScript() == ['grc': 'Νεφέλαι Λυσιστράτη Swedish Svenska', 'grc-Latn-t-grc-Grek-x0-skr-1980': 'Nephelai Lysistratē Swedish Svenska']

        when:
        thing = [
                '@type'   : 'Work',
                'hasTitle': [['@type'          : 'Title',
                              'mainTitleByLang': ["grc": "Νεφέλαι", "grc-Latn-t-grc-Grek-x0-skr-1980": "Nephelai"],
                              'subtitleByLang' : ["grc": "Λυσιστράτη", "grc-Latn-t-grc-Grek-x0-skr-1980": "Lysistratē"]]],
                'language': [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ]
        ]
        chip = fresnel.applyLens(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)

        then:
        chip.byScript() == ['grc': 'Νεφέλαι Λυσιστράτη Swedish Svenska', 'grc-Latn-t-grc-Grek-x0-skr-1980': 'Nephelai Lysistratē Swedish Svenska']
    }

    def "search-tokens"() {
        given:
        var fresnel = new FresnelUtil(ld)
        var thing
        var searchToken

        when:
        thing = [
                '@type'   : 'Work',
                'hasTitle': [['@type': 'Title', 'mainTitle': 'Titel'], ['@type': 'VariantTitle', 'mainTitle': 'variant title']],
                'language': [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ]
        ]
        searchToken = fresnel.applyLens(thing, FresnelUtil.Lenses.SEARCH_TOKEN, [FresnelUtil.Options.NO_FALLBACK])

        then:
        searchToken.asString() == "Titel"

        when:
        thing = [
                '@type'     : 'Person',
                'givenName' : 'Namn',
                'familyName': 'Namnsson',
                'lifeSpan'  : '1990-'
        ]
        searchToken = fresnel.applyLens(thing, FresnelUtil.Lenses.SEARCH_TOKEN, [FresnelUtil.Options.NO_FALLBACK])

        then:
        searchToken.asString() == "Namn Namnsson"

        when:
        thing = [
                '@type'    : 'Topic',
                'prefLabel': 'Hästar'
        ]
        searchToken = fresnel.applyLens(thing, FresnelUtil.Lenses.SEARCH_TOKEN, [FresnelUtil.Options.NO_FALLBACK])

        then:
        searchToken.asString() == ""

        when:
        thing = [
                '@type'    : 'Topic',
                'prefLabel': 'Hästar'
        ]
        searchToken = fresnel.applyLens(thing, FresnelUtil.Lenses.SEARCH_TOKEN)

        then:
        searchToken.asString() == "Hästar"
    }

    def "inverse integral"() {
        given:
        def vocab = [
                "@graph": [
                        [
                                "@id"      : "http://example.org/ns/hasInstance",
                                "category" : ["@id": "http://example.org/ns/integral"],
                                "inverseOf": ["@id": "http://example.org/ns/instanceOf"]
                        ],
                        [
                                "@id"      : "http://example.org/ns/instanceOf",
                                "inverseOf": ["@id": "http://example.org/ns/hasInstance"]
                        ]
                ]
        ]
        def displayData = [
                "@context"  : [

                ],
                "lensGroups": [
                        "chips"       :
                                ["lenses": [
                                        "Instance": [
                                                "@id"            : "Instance-chips",
                                                "@type"          : "fresnel:Lens",
                                                "classLensDomain": "Instance",
                                                "showProperties" : [
                                                        [
                                                                "alternateProperties": [
                                                                        ["subPropertyOf": "hasTitle", "range": "KeyTitle"],
                                                                        ["subPropertyOf": "hasTitle", "range": "Title"],
                                                                        "hasTitle",
                                                                        "identifiedBy"
                                                                ]
                                                        ]
                                                ]
                                        ],
                                        "ISBN"    : [
                                                "@id"            : "ISBN-chips",
                                                "@type"          : "fresnel:Lens",
                                                "classLensDomain": "ISBN",
                                                "showProperties" : ["value"]
                                        ],
                                        "Title"   : [
                                                "@id"            : "Title-chips",
                                                "@type"          : "fresnel:Lens",
                                                "classLensDomain": "Title",
                                                "showProperties" : ["mainTitle"]
                                        ],
                                        "Work"    : [
                                                "@id"            : "Work-chips",
                                                "@type"          : "fresnel:Lens",
                                                "classLensDomain": "Work",
                                                "showProperties" : ["hasTitle"]
                                        ],
                                ]],
                        "cards"       :
                                ["lenses": [
                                        "Instance": [
                                                "@id"            : "Instance-cards",
                                                "@type"          : "fresnel:Lens",
                                                "classLensDomain": "Instance",
                                                "showProperties" : [
                                                        [
                                                                "alternateProperties": [
                                                                        ["subPropertyOf": "hasTitle", "range": "KeyTitle"],
                                                                        ["subPropertyOf": "hasTitle", "range": "Title"],
                                                                        "hasTitle",
                                                                        "identifiedBy"
                                                                ]
                                                        ],
                                                        "responsibilityStatement"
                                                ]
                                        ],
                                        "Work"    : [
                                                "@id"            : "Work-cards",
                                                "@type"          : "fresnel:Lens",
                                                "classLensDomain": "Work",
                                                "showProperties" : [
                                                        "hasTitle",
                                                        "language",
                                                        ["inverseOf": "instanceOf"]
                                                ]
                                        ]
                                ]],
                        "search-cards":
                                ["lenses": [
                                        "Instance": [
                                                "fresnel:extends": ["@id": "Instance-cards"],
                                                "classLensDomain": "Instance",
                                                "showProperties" : [
                                                        "fresnel:super",
                                                        "editionStatement"
                                                ]
                                        ],
                                        "Work"    : [
                                                "fresnel:extends": ["@id": "Work-cards"],
                                                "classLensDomain": "Instance",
                                                "showProperties" : [
                                                        "fresnel:super",
                                                        "originDate"
                                                ]
                                        ]
                                ]]
                ]
        ]
        var fresnel = new FresnelUtil(new JsonLd(CONTEXT_DATA, displayData, vocab))
        var work = [
                '@type'     : 'Work',
                'hasTitle'  : [['@type': 'Title', 'mainTitle': 'Verkstitel']],
                'language'  : [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': ['en': 'Swedish', 'sv': 'Svenska']],
                ],
                'originDate': '2025',
                '@reverse'  : [
                        'instanceOf': [
                                '@type'                  : 'Instance',
                                'responsibilityStatement': 'av Någon',
                                'hasTitle'               : [['@type': 'Title', 'mainTitle': 'Instanstitel']],
                                'identifiedBy'           : ['@type': 'ISBN', 'value': '9789178034239'],
                                'editionStatement'       : "upplagan"
                        ]
                ]
        ]

        when:
        var chipStr = fresnel.applyLens(work, FresnelUtil.NestedLenses.CHIP_TO_TOKEN).asString()
        var cardStr = fresnel.applyLens(work, FresnelUtil.NestedLenses.CARD_TO_CHIP_TO_TOKEN).asString()
        var cardStrAlt = fresnel.applyLens(work, FresnelUtil.NestedLenses.CARD_TO_CHIP_TO_TOKEN, [FresnelUtil.Options.TAKE_ALL_ALTERNATE]).asString()
        var cardOnlyStr = fresnel.applyLens(work, CARD_ONLY).asString()
        var cardOnlyStrAlt = fresnel.applyLens(work, CARD_ONLY, [FresnelUtil.Options.TAKE_ALL_ALTERNATE]).asString()
        var searchCardStr = fresnel.applyLens(work, FresnelUtil.NestedLenses.SEARCH_CARD_TO_SEARCH_CHIP).asString()
        var searchCardOnlyStr = fresnel.applyLens(work, SEARCH_CARD_ONLY).asString()

        then:
        chipStr == "Verkstitel"
        cardStr == "Verkstitel Swedish Svenska Instanstitel av Någon"
        cardStrAlt == "Verkstitel Swedish Svenska Instanstitel Instanstitel 9789178034239 av Någon"
        cardOnlyStr == "Swedish Svenska Instanstitel av Någon"
        cardOnlyStrAlt == "Swedish Svenska Instanstitel Instanstitel 9789178034239 av Någon"
        searchCardStr == "Verkstitel Swedish Svenska Instanstitel av Någon upplagan 2025"
        searchCardOnlyStr == "upplagan 2025"

        when:
        var chip = fresnel.getLensedThing(work, FresnelUtil.NestedLenses.CHIP_TO_TOKEN)
        var card = fresnel.getLensedThing(work, FresnelUtil.NestedLenses.CARD_TO_CHIP_TO_TOKEN)
        var searchCard = fresnel.getLensedThing(work, FresnelUtil.NestedLenses.SEARCH_CARD_TO_SEARCH_CHIP)
        var cardOnly = fresnel.getLensedThing(work, CARD_ONLY)
        var searchCardOnly = fresnel.getLensedThing(work, SEARCH_CARD_ONLY)

        then:
        chip == [
                "@type"   : "Work",
                "hasTitle": [
                        "@type"    : "Title",
                        "mainTitle": "Verkstitel"
                ]
        ]
        card == [
                "@type"   : "Work",
                "hasTitle": [
                        "@type"    : "Title",
                        "mainTitle": "Verkstitel"
                ],
                "language": [
                        "@type"      : "Language",
                        "labelByLang": [
                                "en": "Swedish"
                        ]
                ],
                "@reverse": [
                        "instanceOf": [
                                "@type"                  : "Instance",
                                "hasTitle"               : [
                                        "@type"    : "Title",
                                        "mainTitle": "Instanstitel"
                                ],
                                "identifiedBy"           : [
                                        "@type": "ISBN",
                                        "value": "9789178034239"
                                ],
                                "responsibilityStatement": "av Någon"
                        ]
                ]
        ]
        searchCard == [
                "@type"     : "Work",
                "hasTitle"  : [
                        "@type"    : "Title",
                        "mainTitle": "Verkstitel"
                ],
                "language"  : [
                        "@type"      : "Language",
                        "labelByLang": [
                                "en": "Swedish"
                        ]
                ],
                "@reverse"  : [
                        "instanceOf": [
                                "@type"                  : "Instance",
                                "hasTitle"               : [
                                        "@type"    : "Title",
                                        "mainTitle": "Instanstitel"
                                ],
                                "identifiedBy"           : [
                                        "@type": "ISBN",
                                        "value": "9789178034239"
                                ],
                                "responsibilityStatement": "av Någon",
                                "editionStatement"       : "upplagan"
                        ]
                ],
                "originDate": "2025"
        ]
        cardOnly == [
                "@type"   : "Work",
                "language": [
                        "@type"      : "Language",
                        "labelByLang": [
                                "en": "Swedish"
                        ]
                ],
                "@reverse": [
                        "instanceOf": [
                                "@type"                  : "Instance",
                                "hasTitle"               : [
                                        "@type"    : "Title",
                                        "mainTitle": "Instanstitel"
                                ],
                                "identifiedBy"           : [
                                        "@type": "ISBN",
                                        "value": "9789178034239"
                                ],
                                "responsibilityStatement": "av Någon"
                        ]
                ]
        ]
        searchCardOnly == [
                "@type"     : "Work",
                "@reverse"  : [
                        "instanceOf": [
                                "@type"           : "Instance",
                                "editionStatement": "upplagan"
                        ]
                ],
                "originDate": "2025"
        ]
    }
}

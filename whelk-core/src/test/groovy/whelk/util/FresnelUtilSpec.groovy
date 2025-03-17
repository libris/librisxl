package whelk.util

import spock.lang.Specification
import whelk.JsonLd

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
                    "@vocab": "http://example.org/ns/",
                    "pfx": "http://example.org/pfx/",
                    "labelByLang": ["@id": "label", "@container": "@language"],
                    "prefLabelByLang": ["@id": "prefLabel", "@container": "@language"],
                    "mainTitleByLang": ["@id": "mainTitle", "@container": "@language"],
                    "subtitleByLang": ["@id": "subtitle", "@container": "@language"],
                    "partNameByLang": ["@id": "partName", "@container": "@language"],
                    "partNumberByLang": ["@id": "partNumber", "@container": "@language"],
                    "titleRemainderByLang": ["@id": "titleRemainder", "@container": "@language"],
                    "responsibilityStatementByLang": ["@id": "responsibilityStatement", "@container": "@language"],

                    "issuanceType": [ "@type": "@vocab" ],
                    "langCodeBib": [ "@id": "code", "@type": "ISO639-2-B" ],

            ]
    ]

    static final Map VOCAB_DATA = [
            "@graph": [
                    ["@id": "http://example.org/ns/Resource"],

                    ["@id": "http://example.org/ns/Identity",
                     "subClassOf": [ ["@id": "http://example.org/ns/Resource"] ]],

                    ["@id": "http://example.org/ns/Thing",
                     "subClassOf": [ ["@id": "http://example.org/ns/Resource"] ]],
                    ["@id": "http://example.org/ns/Work",
                     "subClassOf": [ ["@id": "http://example.org/ns/Resource"] ]],
                    ["@id": "http://example.org/ns/StructuredValue",
                     "subClassOf": [ ["@id": "http://example.org/ns/Resource"] ]],

                    ["@id": "http://example.org/ns/Identifier",
                     "subClassOf": [ ["@id": "http://example.org/ns/StructuredValue"] ]],

                    ["@id": "http://example.org/ns/ISBN",
                     "@type": "Class",
                     "subClassOf": [ ["@id": "http://example.org/ns/Identifier"] ]],
                    ["@id": "http://example.org/ns/ISNI",
                     "subClassOf": [ ["@id": "http://example.org/ns/Identifier"] ]],
                    ["@id": "http://example.org/ns/ORCID",
                     "subClassOf": [ ["@id": "http://example.org/ns/Identifier"] ]],
                    ["@id": "http://example.org/ns/MatrixNumber",
                     "@type": "Class",
                     "subClassOf": [ ["@id": "http://example.org/ns/Identifier"] ],
                     "labelByLang": [ "en": "Audio matrix number", "sv": "Matrisnummer"],
                    ],

                    ["@id": "http://example.org/ns/Title",
                     "subClassOf": [ ["@id": "http://example.org/ns/StructuredValue"] ]],
                    ["@id": "http://example.org/ns/KeyTitle",
                     "subClassOf": [ ["@id": "http://example.org/ns/Title"] ]],
                    ["@id": "http://example.org/ns/VariantTitle",
                     "subClassOf": [ ["@id": "http://example.org/ns/Title"] ]],

                    ["@id": "http://example.org/ns/TitlePart",
                     "subClassOf": [ ["@id": "http://example.org/ns/Resource"] ]],

                    ["@id": "http://example.org/ns/Contribution",
                     "subClassOf": [ ["@id": "http://example.org/ns/Resource"] ]],
                    ["@id": "http://example.org/ns/PrimaryContribution",
                     "subClassOf": [ ["@id": "http://example.org/ns/Contribution"] ]],
                    ["@id": "http://example.org/ns/Language",
                     "subClassOf": [ ["@id": "http://example.org/ns/Resource"] ]],

                    ["@id": "http://example.org/ns/Person",
                     "subClassOf": [ ["@id": "http://example.org/ns/Identity"] ]],

                    ["@id": "http://example.org/ns/Serial",
                     "@type": "Class",
                     "subClassOf": [ ["@id": "http://example.org/ns/Resource"] ],
                     "labelByLang": [ "en": "Serial", "sv": "Seriell resurs" ],
                    ],

                    ["@id": "http://example.org/ns/Publication",
                     "subClassOf": ["@id": "http://example.org/ns/ProvisionActivity"]],
                    ["@id": "http://example.org/pfx/SpecialPublication",
                     "subClassOf": ["@id": "http://example.org/ns/Publication"]],

                    ["@id": "http://example.org/ns/label", "@type": ["@id": "DatatypeProperty" ]],
                    ["@id": "http://example.org/ns/prefLabel",
                     "subPropertyOf": [ ["@id": "http://example.org/ns/label"] ]],
                    ["@id": "http://example.org/ns/preferredLabel",
                     "subPropertyOf": ["@id": "http://example.org/ns/prefLabel"]],
                    ["@id": "http://example.org/ns/name",
                     "subPropertyOf": ["@id": "http://example.org/ns/label"]]
            ]
    ]

    static final Map DISPLAY_DATA = [
            "@context": [

            ],
            "lensGroups": [
                    "tokens":
                            ["lenses": [
                                    "Contribution": [
                                            "@id": "Contribution-tokens",
                                            "@type": "fresnel:Lens",
                                            "classLensDomain": "Contribution",
                                            "showProperties": [ "agent" ]
                                    ],
                            ]],
                    "chips":
                             ["lenses": [
                                     "Contribution": [
                                             "@id": "Contribution-chips",
                                             "@type": "fresnel:Lens",
                                             "classLensDomain": "Contribution",
                                             "showProperties": [ "agent", "role" ]
                                     ],
                                     "Thing": ["showProperties": [
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
                                     "Person": [
                                             "@id": "Person-chips",
                                             "@type": "fresnel:Lens",
                                             "classLensDomain": "Person",
                                             "showProperties": [ "familyName", "givenName", "name", "marc:numeration", "lifeSpan", "marc:titlesAndOtherWordsAssociatedWithAName", "fullerFormOfName" ]
                                     ],
                                     "Title": [
                                             "@id": "Title-chips",
                                             "@type": "fresnel:Lens",
                                             "classLensDomain": "Title",
                                             "showProperties": [ "mainTitle", "title", "subtitle", "titleRemainder", "partNumber", "partName", "hasPart" ]
                                     ],
                                     "TitlePart": [
                                             "@id": "TitlePart-chips",
                                             "@type": "fresnel:Lens",
                                             "classLensDomain": "TitlePart",
                                             "showProperties": ["partNumber", "partName"]
                                     ],
                                     "Identifier": [
                                         "@id": "Identifier-chips",
                                         "@type": "fresnel:Lens",
                                         "classLensDomain": "Identifier",
                                         "showProperties": [
                                                 "rdf:type",
                                                 ["alternateProperties": [ "value", "marc:hiddenValue" ]],
                                                 "typeNote",
                                                 "hasNote"
                                         ]
                                     ],
                                     "Work": [
                                         "@id": "Work-chips",
                                         "@type": "fresnel:Lens",
                                         "classLensDomain": "Work",
                                         "showProperties": [
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
                    "cards":
                            ["lenses": [
                                    "Work": [
                                            "@id": "Work-cards",
                                            "@type": "fresnel:Lens",
                                            "classLensDomain": "Work",
                                            "showProperties": [
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
            "formatters" : [
                    'commaBeforeProperty-format': ['TODO': 'fullerFormOfName skrivs alltid med parenteser i libris nu, ska det vara så?', '@id': 'commaBeforeProperty-format', '@type': 'fresnel:Format', 'fresnel:propertyFormatDomain': ['lifeSpan', 'familyName', 'givenName', 'name', 'marc:numeration', 'lifeSpan', 'marc:titlesAndOtherWordsAssociatedWithAName', 'fullerFormOfName'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ', ', 'fresnel:contentFirst': '']],
                    'subtitle-format': ['@id': 'subtitle-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Title'], 'fresnel:propertyFormatDomain': ['subtitle', 'titleRemainder'], 'fresnel:propertyStyle': ['font-normal'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' : ', 'fresnel:contentFirst': '']],
                    'Title-qualifier-format': ['@id': 'Title-qualifier-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Title'], 'fresnel:propertyFormatDomain': ['qualifier'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' ', 'fresnel:contentFirst': '']],
                    'transliteration': ['@id': 'transliteration', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['_Transliterated'], 'fresnel:propertyFormatDomain': ['_transliterations'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' (', 'fresnel:contentFirst': '(', 'fresnel:contentAfter': ')', 'fresnel:contentLast': ')']],
                    'default-separators': ['@id': 'default-separators', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Resource'], 'fresnel:propertyFormatDomain': ['*'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' • ', 'fresnel:contentFirst': ''], 'fresnel:valueFormat': ['fresnel:contentBefore': ', ', 'fresnel:contentFirst': '']],
                    'Identifier-format': ['@id': 'Identifier-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Identifier'], 'fresnel:resourceStyle': ['ext-link', 'displayType()', 'uriToId()']],
                    'ISNI-digits-format': ['@id': 'ISNI-digits-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['ISNI', 'ORCID'], 'fresnel:propertyFormatDomain': ['value'], 'fresnel:valueStyle': ['isniGroupDigits()']],
                    'Identifier-value-format': ['@id': 'Identifier-value-format', '@type': 'fresnel:Format', 'fresnel:classFormatDomain': ['Identifier'], 'fresnel:propertyFormatDomain': ['value'], 'fresnel:propertyFormat': ['fresnel:contentBefore': ' ', 'fresnel:contentFirst': '']],
                    'hasTitle-format': ['@id': 'hasTitle-format', '@type': 'fresnel:Format', 'fresnel:propertyFormatDomain': ['hasTitle'], 'fresnel:valueFormat': ['fresnel:contentBefore': ' = ', 'fresnel:contentFirst': '']],
            ]
    ]

    // TODO JsonLd modifies DISPLAY_DATA every time it is loaded
    static final var ld = new JsonLd(CONTEXT_DATA, DISPLAY_DATA, VOCAB_DATA)

    def "format"() {
        given:
        var fresnel = new FresnelUtil(ld)
        var thing = [
                "@type": "Thing",
                "notation": "NOTATION",
                "labelByLang": ["sv": "etikett", "en": "label"],
                "title": [
                        "@type": "Title",
                        "mainTitleByLang": [
                                "el": "Το νησι των θησαυρων",
                                "el-Latn-t-el-Grek-x0-btj": "To nisi ton thisavron"]],
                "note": ["NOTE 1", "NOTE 2"]
        ]

        var lensed = fresnel.applyLens(thing, FresnelUtil.LensGroupName.Chip)
        var result = fresnel.format(lensed, new FresnelUtil.LangCode('sv'))

        expect:
        result.asString() == "NOTATION • etikett • NOTE 1, NOTE 2 • Το νησι των θησαυρων ’To nisi ton thisavron’"
    }

    def "Heliogabalus"() {
        given:
        var fresnel = new FresnelUtil(ld)
        var thing = [
                '@id':'https://libris-qa.kb.se/khw03jc347kgv4w#it',
                'name':'Heliogabalus',
                '@type':'Person',
                'image':[['@id':'https://libris.kb.se/dataset/images/8d35730bbea96c833b7a54124eeecd9d.full.jpg']],
                'sameAs':[['@id':'http://libris.kb.se/resource/auth/281943']],
                'lifeSpan':'203-222',
                'exactMatch':[['@id':'http://www.wikidata.org/entity/Q1762']],
                'hasVariant':[['name':'Elagabalus', '@type':'Person', 'lifeSpan':'203-222', 'marc:titlesAndOtherWordsAssociatedWithAName':['romersk kejsare']], ['name':'Marcus Aurelius Antoninus', '@type':'Person', 'lifeSpan':'203-222', 'marc:titlesAndOtherWordsAssociatedWithAName':['romersk kejsare']], ['@type':'Person', 'lifeSpan':'203-222', 'givenName':'Varius Avitus', 'familyName':'Bassianus', 'marc:titlesAndOtherWordsAssociatedWithAName':['romersk kejsare']], ['name':'Héliogabale', '@type':'Person', 'lifeSpan':'203-222', 'marc:titlesAndOtherWordsAssociatedWithAName':['romersk kejsare']], ['name':'Elagabale', '@type':'Person', 'lifeSpan':'203-222', 'marc:titlesAndOtherWordsAssociatedWithAName':['romersk kejsare']], ['name':'El Gabal', '@type':'Person', 'lifeSpan':'203-222', 'marc:titlesAndOtherWordsAssociatedWithAName':['romersk kejsare']], ['name':'Elagabalo', '@type':'Person', 'lifeSpan':'203-222', 'marc:titlesAndOtherWordsAssociatedWithAName':['romersk kejsare']]],
                'description':['Romersk kejsare från 218; likviderad'],
                'nationality':[['@id':'https://id.kb.se/nationality/xx']],
                'identifiedBy':[['@type':'VIAF', 'value':'24583475'], ['@type':'ISNI', 'value':'0000000103407488']],
                'marc:titlesAndOtherWordsAssociatedWithAName':['romersk kejsare']
        ]

        var result = fresnel.format(fresnel.applyLens(thing, FresnelUtil.LensGroupName.Chip), new FresnelUtil.LangCode('sv'))

        expect:
        result.asString() == "Heliogabalus, 203-222, romersk kejsare"
    }

    def "complex title"() {
        given:
        var thing = [
                '@type':'Title',
                'mainTitle':'Teater-biblioteket',
                'hasPart':[
                        [
                                '@type':'TitlePart',
                                'partName':['En friare i lifsfara : skämt med sång i en akt'],
                                'partNumber':['N:o 15']
                        ]
                ]
        ]

        var fresnel = new FresnelUtil(ld)
        var result = fresnel.format(fresnel.applyLens(thing, FresnelUtil.LensGroupName.Chip), new FresnelUtil.LangCode('sv'))

        expect:
        result.asString() == "Teater-biblioteket • N:o 15 • En friare i lifsfara : skämt med sång i en akt"
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

        var result = fresnel.format(fresnel.applyLens(thing, FresnelUtil.LensGroupName.Chip), new FresnelUtil.LangCode('sv'))

        expect:
        result.asString() == "Νεφέλαι : Λυσιστράτη ’Nephelai : Lysistratē’"
    }

    def "showProperties rdf:type"() {
        given:
        var fresnel = new FresnelUtil(ld)

        expect:
        fresnel.format(fresnel.applyLens(thing, FresnelUtil.LensGroupName.Chip), new FresnelUtil.LangCode(locale)).asString() == result

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
        fresnel.format(fresnel.applyLens(thing, FresnelUtil.LensGroupName.Chip), new FresnelUtil.LangCode(locale)).asString() == result

        where:
        thing                                        | locale | result
        ['@type': 'Thing', 'issuanceType': 'Serial'] | 'sv'   | 'Seriell resurs'
        ['@type': 'Thing', 'issuanceType': 'Serial'] | 'en'   | 'Serial'
    }


    static var kt = ['@type': 'KeyTitle', 'mainTitle': 'key title']
    static var kt2 = ['@type': 'KeyTitle', 'mainTitle': 'key title 2']
    static var tt = ['@type': 'Title', 'mainTitle': 'title']
    static var vt  = ['@type': 'VariantTitle', 'mainTitle': 'variant title']

    def "alternateProperties + range restriction"() {
        given:
        var fresnel = new FresnelUtil(ld)

        expect:
        fresnel.format(fresnel.applyLens(thing, FresnelUtil.LensGroupName.Chip), new FresnelUtil.LangCode(locale)).asString() == result

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

    def "isniGroupDigits()"() {
        given:
        var thing = [
                '@type': 'ISNI',
                'value': '0000000103407488'
        ]

        var fresnel = new FresnelUtil(ld)
        var result = fresnel.format(fresnel.applyLens(thing, FresnelUtil.LensGroupName.Chip), new FresnelUtil.LangCode('sv'))

        expect:
        result.asString() == "ISNI 0000 0001 0340 7488"
    }

    def "chips vs tokens"() {
        given:
        var thing = [
                '@type': 'Work',
                'hasTitle': [
                        ['@type': 'Title', 'mainTitle': 'Titel'],
                        ['@type': 'Variant', 'mainTitle': 'variant title']
                ],
                'language' : [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': [ 'en': 'Swedish', 'sv': 'Svenska' ]],
                ],
                'contribution' : [
                        ['@type': 'Contribution', 'role': 'translator', 'agent': [ '@type': 'Person', 'name': 'Överzet' ]],
                        ['@type': 'PrimaryContribution', 'role': 'author', 'agent': [ '@type': 'Person', 'givenName': 'Namn', 'familyName': 'Namnsson', 'lifeSpan': "1972-"]],
                ]
        ]

        var fresnel = new FresnelUtil(ld)
        var result = fresnel.format(fresnel.applyLens(thing, FresnelUtil.LensGroupName.Chip), new FresnelUtil.LangCode('sv'))
        expect:
        result.asString() == "Titel • Svenska • Namnsson, Namn, 1972-"
    }

    def "derived lens"() {
        given:
        var thing = [
                '@type': 'Work',
                'hasTitle': [
                        ['@type': 'Title', 'mainTitle': 'Titel'],
                        ['@type': 'VariantTitle', 'mainTitle': 'variant title']
                ],
                'language' : [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': [ 'en': 'Swedish', 'sv': 'Svenska' ]],
                ],
                'subject' : [
                        ['@type': 'Topic', 'prefLabel': "Hästar"],
                ],
                'contribution' : [
                        ['@type': 'Contribution', 'role': 'translator', 'agent': [ '@type': 'Person', 'name': 'Överzet' ]],
                        ['@type': 'PrimaryContribution', 'role': 'author', 'agent': [ '@type': 'Person', 'givenName': 'Namn', 'familyName': 'Namnsson', 'lifeSpan': "1972-"]],
                ]
        ]

        var fresnel = new FresnelUtil(ld)
        var cardOnly = new FresnelUtil.DerivedLens(
                FresnelUtil.LensGroupName.Card,
                [FresnelUtil.LensGroupName.Chip],
                FresnelUtil.LensGroupName.Token
        )

        var result = fresnel.applyLens(thing, cardOnly)
        expect:
        result.asString() == "Överzet Namnsson Namn 1972- Hästar"
    }

    def "take all alternate"() {
        given:
        var thing = [
                '@type': 'Work',
                'hasTitle': [
                        ['@type': 'Title', 'mainTitle': 'Titel'],
                        ['@type': 'VariantTitle', 'mainTitle': 'variant title']
                ],
                'language' : [
                        ['@type': 'Language', 'code': 'sv', 'labelByLang': [ 'en': 'Swedish', 'sv': 'Svenska' ]],
                ],
                'subject' : [
                        ['@type': 'Topic', 'prefLabel': "Hästar"],
                ],
                'contribution' : [
                        ['@type': 'Contribution', 'role': 'translator', 'agent': [ '@type': 'Person', 'name': 'Överzet' ]],
                        ['@type': 'PrimaryContribution', 'role': 'author', 'agent': [ '@type': 'Person', 'givenName': 'Namn', 'familyName': 'Namnsson', 'lifeSpan': "1972-"]],
                ]
        ]
        var fresnel = new FresnelUtil(ld)

        var result = fresnel.applyLens(thing, FresnelUtil.LensGroupName.Card, FresnelUtil.Options.TAKE_ALL_ALTERNATE)

        expect:
        result.asString() == "Titel Titel variant title Överzet translator Namnsson Namn 1972- author Svenska Swedish Hästar"
    }
}

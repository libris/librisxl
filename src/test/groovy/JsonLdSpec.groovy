package whelk.util

import org.apache.commons.io.IOUtils
import spock.lang.Specification
import org.codehaus.jackson.map.*
import whelk.JsonLd


class JsonLdSpec extends Specification {

    def mapper

    def setup() {
        mapper = new ObjectMapper()
    }

    def "should frame flat jsonld"() {
        given:
        def flatJson = mapper.readValue(loadJsonLdFile("1_flat.jsonld"), Map)
        def framedJson = mapper.readValue(loadJsonLdFile("1_framed.jsonld"), Map)
        when:
        def resultJson = JsonLd.frame("/bib/13531679", flatJson)
        then:
        resultJson.get("about").get("identifier")[0].get("identifierValue") == "9789174771107"
        resultJson.get("about").get("genre")[0].get("prefLabel") == "E-böcker"
        resultJson.equals(framedJson)

    }

    def "should flatten framed jsonld"() {
        given:
        def flatJson = mapper.readValue(defaultFlatRecord, Map)
        def framedJson = mapper.readValue(defaultFramedRecord, Map)
        when:
        def resultJson = JsonLd.flatten(framedJson)
        then:
        flatJson.size() == resultJson.size()
    }

    def "should detect flat jsonld"() {
        given:
        def flatJson = mapper.readValue(defaultFlatRecord, Map)
        def framedJson = mapper.readValue(defaultFramedRecord, Map)
        expect:
        JsonLd.isFlat(flatJson) == true
        JsonLd.isFlat(framedJson) == false
    }

    static String loadJsonLdFile(String fileName) {
        InputStream is = JsonLdSpec.class.getClassLoader().getResourceAsStream(fileName)
        return IOUtils.toString(is, "UTF-8")
    }

    static String defaultFlatRecord = """
    {
        '@graph': [
        {
            '@id': '/bib/13531679',
            '@type': 'Record',
            'about': {
                '@id': '/resource/bib/13531679'
            },
            'catForm': {
                '@id': '/def/enum/record/AACR2'
            },
            'controlNumber': '13531679',
            'created': '2013-03-02T00:00:00.0+01:00',
            'encLevel': {
                '@id': '/def/enum/record/AbbreviatedLevel'
            },
            'modified': '2015-06-15T11:53:42.820000000+02:00'
        },
        {
            '@id': '/resource/bib/13531679',
            '@type': [
            'Text',
            'Monograph'
            ],
            'additionalCarrierType': {
                '@id': '/def/enum/content/Online'
            },
            'attributedTo': {
                '@id': '/resource/auth/350582'
            },
            'author': [
            {
                '@id': '/resource/auth/350582'
            }
            ],
            'genre': [
            {
                '@id': '/topic/saogf/E-b%C3%B6cker'
            },
            {
                '@id': '/resource/auth/315873'
            }
            ],
            'hasFormat': [
            {
                '@type': [
                'Electronic'
                ],
                'carrierType': [
                {
                    '@id': '/def/enum/carrier/OnlineResource'
                }
                ]
            }
            ],
            'identifier': [
            {
                '@type': 'Identifier',
                'identifierScheme': {
                    '@id': '/def/identifiers/isbn'
                },
                'identifierValue': '9789174771107'
            }
            ],
            'instanceTitle': {
                '@type': 'Title',
                'titleValue': 'D\u00e4r ute i m\u00f6rkret'
            },
            'language': [
            {
                '@id': '/def/languages/swe'
            }
            ],
            'literaryForm': {
                '@id': '/def/enum/content/Novel'
            },
            'mediaTerm': 'Elektronisk resurs',
            'publication': [
            {
                '@type': 'ProviderEvent',
                'providerDate': '2012',
                'providerName': 'Pupill F\u00f6rlag'
            }
            ],
            'publicationCountry': [
            {
                '@id': '/def/countries/sw'
            }
            ],
            'subject': [
            {
                '@id': '/topic/bicssc/5AQ'
            },
            {
                '@id': '/topic/bicssc/FA'
            },
            {
                '@id': '/topic/bicssc/FF'
            },
            {
                '@id': '/topic/bicssc/FL'
            }
            ],
            'summary': [
            'Micke \u00e4r deprimerad. Han har precis gjort slut med sin tjej efter sju \u00e5r tillsammans. F\u00f6r att f\u00e5 honom p\u00e5 b\u00e4ttre hum\u00f6r tar hans kompisar med honom p\u00e5 en \u00e4ventyrsresa till den norrl\u00e4ndska vildmarken.Isolerad mitt i skogen ligger den lilla byn Mossfors. Dit anl\u00e4nder de fyra stockholmarna och tas emot av \u00e4ventyrsguiden Angelica som desperat f\u00f6rs\u00f6ker f\u00e5 sitt f\u00f6retag att g\u00e5 ihop.Till en b\u00f6rjan g\u00e5r allt bra, men snart m\u00e4rker de att det \u00e4r n\u00e5got som inte st\u00e4mmer. De ser saker i skogen som de inte kan f\u00f6rklara. Husdjur f\u00f6rsvinner f\u00f6r att senare hittas d\u00f6da, och sakta men s\u00e4kert blir det uppenbart att det finns en varulv d\u00e4r ute i m\u00f6rkret.N\u00e4r str\u00f6mmen f\u00f6rsvinner och mobiltelefonerna slutar fungera m\u00e5ste de samarbeta med ortsbefolkningen f\u00f6r att \u00f6verleva mot en fasa som ingen av dem kunnat f\u00f6rest\u00e4lla sig.F\u00f6rfattarinfo: Markus Sk\u00f6ld bor i Stockholm och jobbar till vardags som systemutvecklare. D\u00e4r ute i m\u00f6rkret \u00e4r hans debutroman. [Elib]'
            ]
        },
        {
            '@type': [
            'Electronic'
            ],
            'carrierType': [
            {
                '@id': '/def/enum/carrier/OnlineResource'
            }
            ]
        },
        {
            '@type': 'Title',
            'titleValue': 'D\u00e4r ute i m\u00f6rkret'
        },
        {
            '@id': '/resource/auth/350582',
            '@type': 'Person',
            'birthYear': '1974',
            'familyName': 'Sk\u00f6ld',
            'givenName': 'Markus'
        },
        {
            '@type': 'ProviderEvent',
            'providerDate': '2012',
            'providerName': 'Pupill F\u00f6rlag'
        },
        {
            '@id': '/topic/saogf/E-b%C3%B6cker',
            '@type': 'Concept',
            'prefLabel': 'E-b\u00f6cker'
        },
        {
            '@id': '/topic/bicssc/FL',
            '@type': 'Concept',
            'inScheme': {
                '@id': '/topic/bicssc'
            },
            'notation': 'FL'
        },
        {
            '@id': '/topic/bicssc',
            '@type': 'ConceptScheme',
            'notation': 'bicssc'
        },
        {
            '@id': '/topic/bicssc/FF',
            '@type': 'Concept',
            'inScheme': {
                '@id': '/topic/bicssc'
            },
            'notation': 'FF'
        },
        {
            '@id': '/topic/bicssc',
            '@type': 'ConceptScheme',
            'notation': 'bicssc'
        },
        {
            '@id': '/topic/bicssc/FA',
            '@type': 'Concept',
            'inScheme': {
                '@id': '/topic/bicssc'
            },
            'notation': 'FA'
        },
        {
            '@id': '/topic/bicssc',
            '@type': 'ConceptScheme',
            'notation': 'bicssc'
        },
        {
            '@id': '/topic/bicssc/5AQ',
            '@type': 'Concept',
            'inScheme': {
                '@id': '/topic/bicssc'
            },
            'notation': '5AQ'
        },
        {
            '@id': '/topic/bicssc',
            '@type': 'ConceptScheme',
            'notation': 'bicssc'
        },
        {
            '@id': '/def/languages/swe',
            '@type': [
                'Language',
                'Concept'
            ],
            'langCode': 'swe',
            'langTag': 'sv',
            'matches': 'http://id.loc.gov/vocabulary/iso639-2/swe',
            'notation': 'swe',
            'prefLabel': 'Svenska',
            'prefLabel_en': 'Swedish'
        },
        {
            '@id': '/def/countries/sw',
            '@type': [
                'Concept',
                'Country'
            ],
            'exactMatch': 'http://id.loc.gov/vocabulary/countries/sw',
            'label_en': 'Sweden',
            'notation': 'sw',
            'prefLabel': 'Sverige',
            'prefLabel_en': 'Sweden'
        },
        {
            '@type': 'Identifier',
            'identifierScheme': {
                '@id': '/def/identifiers/isbn'
            },
            'identifierValue': '9789174771107'
        },
        {
            '@id': '/def/enum/content/Online',
            '@type': 'Concept',
            'prefLabel': 'Onlineutg\u00e5va'
        },
        {
            '@id': '/topic/saogf/E-b%C3%B6cker',
            '@type': 'Concept',
            'prefLabel': 'E-b\u00f6cker'
        },
        {
            '@id': '/resource/auth/315873',
            '@type': 'Concept',
            'prefLabel': 'Unga vuxna',
            'sameAs': {
                '@id': '/topic/saogf/Unga%20vuxna'
            }
        },
        {
            '@id': '/def/enum/content/Novel',
            '@type': 'Concept',
            'prefLabel': 'Roman'
        }
        ]
    }""".replaceAll("'", "\"")

    static String defaultFramedRecord = """
    {
        '@id': '/bib/13531679',
        '@type': 'Record',
        'about': {
            '@id': '/resource/bib/13531679',
            '@type': [
                'Text',
                'Monograph'
            ],
            'additionalCarrierType': {
                '@id': '/def/enum/content/Online',
                '@type': 'Concept',
                'prefLabel': 'Onlineutg\u00e5va'
            },
            'attributedTo': {
                '@id': '/resource/auth/350582',
                '@type': 'Person',
                'birthYear': '1974',
                'familyName': 'Sk\u00f6ld',
                'givenName': 'Markus'
            },
            'author': [
                {
                    '@id': '/resource/auth/350582',
                    '@type': 'Person',
                    'birthYear': '1974',
                    'familyName': 'Sk\u00f6ld',
                    'givenName': 'Markus'
                }
            ],
            'genre': [
                {
                    '@id': '/topic/saogf/E-b%C3%B6cker',
                    '@type': 'Concept',
                    'prefLabel': 'E-b\u00f6cker'
                },
                {
                    '@id': '/resource/auth/315873',
                    '@type': 'Concept',
                    'prefLabel': 'Unga vuxna',
                    'sameAs': {
                        '@id': '/topic/saogf/Unga%20vuxna'
                    }
                }
            ],
            'hasFormat': [
                {
                    '@type': [
                        'Electronic'
                    ],
                    'carrierType': [
                        {
                            '@id': '/def/enum/carrier/OnlineResource'
                        }
                    ]
                }
            ],
            'identifier': [
                {
                    '@type': 'Identifier',
                    'identifierScheme': {
                        '@id': '/def/identifiers/isbn'
                    },
                    'identifierValue': '9789174771107'
                }
            ],
            'instanceTitle': {
                '@type': 'Title',
                'titleValue': 'D\u00e4r ute i m\u00f6rkret'
            },
            'language': [
                {
                    '@id': '/def/languages/swe',
                    '@type': [
                        'Language',
                        'Concept'
                    ],
                    'langCode': 'swe',
                    'langTag': 'sv',
                    'matches': 'http://id.loc.gov/vocabulary/iso639-2/swe',
                    'notation': 'swe',
                    'prefLabel': 'Svenska',
                    'prefLabel_en': 'Swedish'
                }
            ],
            'literaryForm': {
                '@id': '/def/enum/content/Novel',
                '@type': 'Concept',
                'prefLabel': 'Roman'
            },
            'mediaTerm': 'Elektronisk resurs',
            'publication': [
                {
                    '@type': 'ProviderEvent',
                    'providerDate': '2012',
                    'providerName': 'Pupill F\u00f6rlag'
                }
            ],
            'publicationCountry': [
                {
                    '@id': '/def/countries/sw',
                    '@type': [
                        'Concept',
                        'Country'
                    ],
                    'exactMatch': 'http://id.loc.gov/vocabulary/countries/sw',
                    'label_en': 'Sweden',
                    'notation': 'sw',
                    'prefLabel': 'Sverige',
                    'prefLabel_en': 'Sweden'
                }
            ],
            'subject': [
                {
                    '@id': '/topic/bicssc/5AQ',
                    '@type': 'Concept',
                    'inScheme': {
                        '@id': '/topic/bicssc',
                        '@type': 'ConceptScheme',
                        'notation': 'bicssc'
                    },
                    'notation': '5AQ'
                },
                {
                    '@id': '/topic/bicssc/FA',
                    '@type': 'Concept',
                    'inScheme': {
                        '@id': '/topic/bicssc',
                        '@type': 'ConceptScheme',
                        'notation': 'bicssc'
                    },
                    'notation': 'FA'
                },
                {
                    '@id': '/topic/bicssc/FF',
                    '@type': 'Concept',
                    'inScheme': {
                        '@id': '/topic/bicssc',
                        '@type': 'ConceptScheme',
                        'notation': 'bicssc'
                    },
                    'notation': 'FF'
                },
                {
                    '@id': '/topic/bicssc/FL',
                    '@type': 'Concept',
                    'inScheme': {
                        '@id': '/topic/bicssc',
                        '@type': 'ConceptScheme',
                        'notation': 'bicssc'
                    },
                    'notation': 'FL'
                }
            ],
            'summary': [
                'Micke \u00e4r deprimerad. Han har precis gjort slut med sin tjej efter sju \u00e5r tillsammans. F\u00f6r att f\u00e5 honom p\u00e5 b\u00e4ttre hum\u00f6r tar hans kompisar med honom p\u00e5 en \u00e4ventyrsresa till den norrl\u00e4ndska vildmarken.Isolerad mitt i skogen ligger den lilla byn Mossfors. Dit anl\u00e4nder de fyra stockholmarna och tas emot av \u00e4ventyrsguiden Angelica som desperat f\u00f6rs\u00f6ker f\u00e5 sitt f\u00f6retag att g\u00e5 ihop.Till en b\u00f6rjan g\u00e5r allt bra, men snart m\u00e4rker de att det \u00e4r n\u00e5got som inte st\u00e4mmer. De ser saker i skogen som de inte kan f\u00f6rklara. Husdjur f\u00f6rsvinner f\u00f6r att senare hittas d\u00f6da, och sakta men s\u00e4kert blir det uppenbart att det finns en varulv d\u00e4r ute i m\u00f6rkret.N\u00e4r str\u00f6mmen f\u00f6rsvinner och mobiltelefonerna slutar fungera m\u00e5ste de samarbeta med ortsbefolkningen f\u00f6r att \u00f6verleva mot en fasa som ingen av dem kunnat f\u00f6rest\u00e4lla sig.F\u00f6rfattarinfo: Markus Sk\u00f6ld bor i Stockholm och jobbar till vardags som systemutvecklare. D\u00e4r ute i m\u00f6rkret \u00e4r hans debutroman. [Elib]'
            ]
        },
        'catForm': {
            '@id': '/def/enum/record/AACR2'
        },
        'controlNumber': '13531679',
        'created': '2013-03-02T00:00:00.0+01:00',
        'encLevel': {
            '@id': '/def/enum/record/AbbreviatedLevel'
        },
        'modified': '2015-06-15T11:53:42.820000000+02:00'
    }""".replaceAll("'", "\"")

}

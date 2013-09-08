package se.kb.libris.whelks.plugin

import spock.lang.*


class MarcFrameConverterSpec extends Specification {

    def converter = new MarcFrameConverter()

    @Unroll
    def "should detect marc type"() {
        expect:
        converter.conversion.getMarcCategory(marc) == type
        where:
        marc                                    | type
        [leader: "01113cz  a2200421n  4500"]    | "auth"
        [leader: "00887cam a2200277 a 4500"]    | "bib"
        [leader: "00187nx  a22000971n44500"]    | "hold"
    }

    def "should convert a simple bib post"() {
        given:
        def marc = [
            leader: "00887cam a2200277 a 4500",
            fields: [
                ["001": "7149593"],
                ["020": ["ind1": " ", "ind2": " ", "subfields": [
                    ["a": "91-0-056322-6 (inb.)"],
                    ["c": "310:00"]
                ]]],
                ["100": ["ind1": "1", "ind2": " ", "subfields": [
                    ["a": "Jansson, Tove,"],
                    ["d": "1914-2001"]
                ]]],
                ["245": ["ind1": "0", "ind2": "0", "subfields": [
                    ["a": "Anteckningar från en ö"],
                    ["c": "Tove Jansson, Tuulikki Pietilä"]
                ]]],
                ["260": ["ind1": " ", "ind2": " ", "subfields": [
                    ["a": "Stockholm"],
                    ["b": "Bonnier"],
                    ["c": "1996"],
                    ["e": "Finland"]
                ]]],
                ["700": ["ind1": "1", "ind2": " ", "subfields": [
                    ["a": "Pietilä, Tuulikki,"],
                    ["d": "1917-"]
                ]]],
            ]
        ]
        when:
        def frame = converter.createFrame(marc)
        then:
        frame == [
            "@id": "/bib/7149593",
            "@type": "Record",
            "controlNumber": "7149593",
            "status": "c",
            "typeOfRecord": "a",
            "bibLevel": "m",
            "characterCoding": "a",
            "catForm": "a",
            "about": [
                "@id": "/resource/bib/7149593",
                "@type": "Book",
                "title": [
                    "@type": "TitleEntity",
                    "titleValue": "Anteckningar från en ö"
                ],
                "responsibilityStatement": "Tove Jansson, Tuulikki Pietilä",
                "publication": [
                    [
                        "@type": "ProviderEvent",
                        "place": ["@type": "Place", "label": "Stockholm"],
                        "providerName": "Bonnier",
                        "providerDate": "1996"
                    ],
                ],
                "manufacture": [
                    [
                        "@type": "ProviderEvent",
                        "place": ["@type": "Place", "label": "Finland"],
                    ],
                ],
                "identifier": [
                    [
                        "@type": "Identifier",
                        "identifierValue": "91-0-056322-6",
                        "identifierScheme": "ISBN",
                        "identifierNote": "inb."
                    ],
                ],
                "instanceOf": [
                    "@type": "Book",
                    "creator": [
                        "@type": "Person",
                        "controlledLabel": "Jansson, Tove, 1914-2001",
                        "birthYear": "1914",
                        "deathYear": "2001",
                        "familyName": "Jansson",
                        "givenName": "Tove"
                    ],
                    "contributorList": [
                        [
                            "@type": "Person",
                            "controlledLabel": "Pietil\u00e4, Tuulikki, 1917-",
                            "birthYear": "1917",
                            "familyName": "Pietil\u00e4",
                            "givenName": "Tuulikki"
                        ],
                    ]
                ],
                "availability": "310:00",
            ]
        ]
    }

    def "should convert a concept auth post"() {
        given:
        def marc = [
            "leader": "01341cz  a2200397n  4500",
            "fields": [
                ["001": "140482"],
                ["005": "20130814170612.0"],
                ["008": "020409 | anznnbabn          |n ana      "],
                ["150": ["subfields": [["a": "Barnpsykologi"]]]],
                ["550": ["subfields": [["a": "Psykologi"], ["w": "g"]]]],
            ]
        ]
        when:
        def frame = converter.createFrame(marc)
        then:
        frame == [
            "@type":"Record",
            "@id": "/auth/140482",
            controlNumber: "140482",
            modified: "2013-08-14T17:06:12.0+0200",
            status: "c",
            typeOfRecord: "z",
            characterCoding: "a",
            encLevel: "n",
            about: [
                "@type": "Concept",
                "@id": "/topic/sao/Barnpsykologi",
                sameAs: ["@id": "/resource/auth/140482"],
                prefLabel: "Barnpsykologi",
                broader: [
                    ["@type": "Concept",
                    "@id": "/topic/sao/Psykologi",
                     prefLabel: "Psykologi"]
                ]
            ]
        ]
    }

    def "should handle complex 260 fields"() {
    /*

    260	_	_	#a London ; #a New York : #b Routledge Falmer ; #a [London] : #b Open University, #c 2002

    260#3
    Upprepade utgivarbyten för fortlöpande resurser:

    260	_	_	#3 Sammanfattad utgivningstid: #a Lund : #b Svenska Clartésektionen, #c 1924- #e (Stockholm : #f Fram)
    260	_	_	#a Lund : #b Svenska Clartésektionen, #c 1924-1925
    260	2	_	#a Lund : #b Svenska Clartéavdelningen, #c 1926-1927
    260	2	_	#a Stockholm : #b Svenska Clartéavdelningen, #c 1928-1931
    260	2	_	#a Stockholm : #b Svenska Clartéförbundet, #c 1932-1953
    260	2	_	#a Hägersten : #b Clarté, #c 1991-1995
    260	3	_	#a Stockholm : #b Clarté, #c 1953-1991, 1995-

    */
    }

}

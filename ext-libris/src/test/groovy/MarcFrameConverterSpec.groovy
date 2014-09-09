package se.kb.libris.whelks.plugin

import spock.lang.*


@Unroll
class MarcFrameConverterSpec extends Specification {

    def converter = new MarcFrameConverter()

    def "should extract #token from #uri"() {
        expect:
        MarcSimpleFieldHandler.extractToken(tplt, uri) == token
        where:
        tplt            | uri               | token
        "/item/{_}"     | "/item/thing"     | "thing"
        "/item/{_}/eng" | "/item/thing/eng" | "thing"
        "/item/{_}/swe" | "/item/thing/eng" | null
    }

    def "should detect marc #type type in leader value #marc.leader"() {
        expect:
        converter.conversion.getMarcCategory(marc.leader) == type
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
        frame == ["@type": "Record",
            "@id": "/bib/7149593",
            status:["@id": "/def/enum/record/CorrectedOrRevised"],
            characterCoding:["@id": "/def/enum/record/UCS-Unicode"],
            catForm:["@id": "/def/enum/record/AACR2"],
            entryMap: "4500",
            controlNumber: "7149593",
            about:["@type": ["Text", "Monograph"],
                attributedTo:["@type": "Person",
                    familyName: "Jansson",
                    givenName: "Tove",
                    birthYear: "1914",
                    deathYear: "2001",
                    controlledLabel: "Jansson, Tove, 1914-2001"],
                identifier:[["@type": "Identifier",
                    identifierValue: "91-0-056322-6",
                    identifierNote: "inb.",
                    identifierScheme:["@id": "/def/identifiers/isbn"]]],
                availability: "310:00",
                instanceTitle:["@type": "Title",
                    titleValue: "Anteckningar från en ö"],
                responsibilityStatement: "Tove Jansson, Tuulikki Pietilä",
                publication:[
                    ["@type": "ProviderEvent",
                        place:["@type": "Place",
                            label: "Stockholm"],
                        providerName: "Bonnier",
                        providerDate: "1996"]],
                manufacture:[
                    ["@type": "ProviderEvent",
                        place:["@type": "Place",
                            label: "Finland"]]],
                influencedBy:[
                    ["@type": "Person",
                        familyName: "Pietilä",
                        givenName: "Tuulikki",
                        birthYear: "1917",
                        controlledLabel: "Pietilä, Tuulikki, 1917-"]],
                "@id": "/resource/bib/7149593"]]
    }

    def "should handle indicator as token map"() {
        given:
        def marc = [
            leader: "00887cam a2200277 a 4500",
            fields: [
                ["001": "0000000"],
                ["024": ["ind1": "3", "ind2": " ",
                        "subfields": [["a": "1234567890"]]]]
            ]
        ]
        when:
        def frame = converter.createFrame(marc)
        then:
        frame.about.identifier[0] == [
            "@type": "Identifier",
            identifierValue: "1234567890",
            identifierScheme:["@id": "/def/identifiers/ean"]
        ]
    }

    def "should group properties into nested entities"() {
        given:
        def marc = [
            leader: "00887cam a2200277 a 4500",
            fields: [
                ["001": "0000000"],
                ["533": ["ind1": " ", "ind2": " ",
                        "subfields": [
                            ["a": "Digitalt faksimil och elektronisk text"],
                            ["c": "Litteraturbanken"],
                            ["d": "2010"] ]]]
            ]
        ]
        when:
        def frame = converter.createFrame(marc)
        then:
        frame.about.reproduction[0] == [
            "@type": "CreativeWork",
            description: "Digitalt faksimil och elektronisk text",
            provider: [
                ["@type": "ProviderEvent",
                    providerDate: "2010",
                    providerName: ["Litteraturbanken"]]]
        ]
    }

    def "should match indicator as property switch"() {
        // TODO
    }

    def "should convert a concept auth post"() {
        given:
        def marc = [
            "leader": "01341cz  a2200397n  4500",
            "fields": [
                ["001": "140482"],
                ["005": "20130814170612.0"],
                ["150": ["subfields": [["a": "Barnpsykologi"]]]],
                ["550": ["subfields": [["a": "Psykologi"], ["w": "g"]]]],
            ]
        ]
        when:
        def frame = converter.createFrame(marc)
        then:
        frame == [
            "@type": "Record",
            "@id": "/auth/140482",
            controlNumber: "140482",
            modified: "2013-08-14T17:06:12.0+0200",
            about: [
                "@type": "Concept",
                "@id": "/resource/auth/140482",
                sameAs: [["@id": "/topic/sao/Barnpsykologi"]],
                prefLabel: "Barnpsykologi",
                broader: [
                    ["@type": "Concept",
                        "@id": "/topic/sao/Psykologi",
                        prefLabel: "Psykologi"]
                ]
            ],
            entryMap: "4500",
            status: ["@id": "/def/enum/record/CorrectedOrRevised"],
            characterCoding: ["@id": "/def/enum/record/UCS-Unicode"]
        ]
    }

    def "should convert dbpedia uri in 035 to sameAs of instance"() {
        given:
        def marc = [
            "leader": "01341cz  a2200397n  4500",
            "fields": [
                ["001": "94541"],
                ["100": ["subfields": [["a": "Strindberg"]]]],
                ["035": ["subfields": [["a": "http://dbpedia.org/resource/August_Strindberg"]]]],
            ]
        ]
        when:
        def frame = converter.createFrame(marc)
        then:
        frame == [
            "@type":"Record",
            "@id": "/auth/94541",
            controlNumber: "94541",
            about: [
                "@type": "Person",
                "@id": "/resource/auth/94541",
                "name": "Strindberg",
                sameAs: [["@id": "http://dbpedia.org/resource/August_Strindberg"]]
            ],
            entryMap: "4500",
            status: ["@id": "/def/enum/record/CorrectedOrRevised"],
            characterCoding: ["@id": "/def/enum/record/UCS-Unicode"]
        ]
    }

    def "should convert concept with scheme in 040"() {
        given:
        def marc = [
            "leader": "01341cz  a2200397n  4500",
            "fields": [
                ["001": "247755"],
                ["005": "20130814170612.0"],
                ["040":["subfields":[["f":"barn"]]]],
                ["008": "020409 | anznnbabn          |n ana      "],
                ["150": ["subfields": [["a": "Pengar"]]]],
                ["550": ["subfields": [["a": "Handel"], ["w": "g"]]]],
            ]
        ]
        when:
        def frame = converter.createFrame(marc)
        then:
        frame == [
            "@type":"Record",
            "@id": "/auth/247755",
            controlNumber: "247755",
            modified: "2013-08-14T17:06:12.0+0200",
            about: [
                "@type": "Concept",
                "@id": "/resource/auth/247755",
                sameAs: [["@id": "/topic/barn/Pengar"]],
                prefLabel: "Pengar",
                inScheme: ["@type": "ConceptScheme", "@id": "/topic/barn", notation: "barn"],
                broader: [
                    ["@type": "Concept",
                    "@id": "/topic/barn/Handel",
                     prefLabel: "Handel"]
                ]
            ],
            entryMap: "4500",
            status: ["@id": "/def/enum/record/CorrectedOrRevised"],
            characterCoding: ["@id": "/def/enum/record/UCS-Unicode"]
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

    def "should store failed marc data"() {
        given:
                //["007": ["subfields": [["?": "?"]]]],
        def marc = [
            leader: "00887cam a2200277 a 4500",
            "fields": [
                ["001": "0000000"],
                ["008": "020409 | anznnbabn          |EEEEEEEEEEE"],
                ["100": "..."],
                ["100": ["subfields": [["?": "?"]]]]
            ]
        ]
        when:
        def frame = converter.createFrame(marc)
        then:
        frame._marcUncompleted == [
            ["008": "020409 | anznnbabn          |EEEEEEEEEEE"],
            ["100": ["subfields": [["?": "?"]]]]
        ]
        frame._marcBroken == [["100": "..."]]
        frame._marcFailedFixedFields == [
            "008": ["38": "E", "39": "E", "29": "E", "30": "E", "31": "E", "34": "E"]
        ]
    }

}

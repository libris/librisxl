package se.kb.libris.whelks.plugin

import spock.lang.*


@Unroll
class MarcFrameConverterSpec extends Specification {

    static converter = new MarcFrameConverter() {
        def config
        void initialize(URIMinter uriMinter, Map config) {
            super.initialize(uriMinter, config)
            this.config = config
        }
    }

    static List fieldSpecs = []
    static marcSkeletons = [:]
    static marcResults = [:]

    static {
        ['bib', 'auth', 'hold'].each { marcType ->
            converter.config[marcType].each { code, field ->
                if (code == '000') {
                    marcSkeletons[marcType] = field._specSource
                    marcResults[marcType] = field._specResult
                }
                if (field._specSource && field._specResult) {
                    fieldSpecs << [source: field._specSource, result: field._specResult,
                                   marcType: marcType, code: code]
                } else if (field._spec instanceof List) {
                    field._spec.each {
                        if (it instanceof Map && it.source && it.result) {
                            fieldSpecs << [source: it.source, result: it.result,
                                           marcType: marcType, code: code]
                        }
                    }
                }
            }
        }
    }

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

    def "should convert field spec #fieldSpec.marcType #fieldSpec.code"() {
        given:
        when:
        def marcType = fieldSpec.marcType
        def marc = deepcopy(marcSkeletons[marcType])
        if (fieldSpec.source.fields) {
            marc.fields = fieldSpec.source.fields
        } else {
            marc.fields << fieldSpec.source
        }
        def result = converter.createFrame(marc)
        def expected = deepcopy(marcResults[marcType])
        // test id generation separately
        expected['@id'] = result['@id']
        expected['about']['@id'] = result['about']['@id']
        fieldSpec.result.each { prop, obj ->
            def value = expected[prop]
            if (value instanceof Map) value.putAll(obj)
            else expected[prop] = obj
        }
        then:
        result == expected
        where:
        fieldSpec << fieldSpecs
    }

    def "should match indicator as property switch"() {
        // TODO
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

    private deepcopy(orig) {
        def bos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(bos)
        oos.writeObject(orig); oos.flush()
        def bin = new ByteArrayInputStream(bos.toByteArray())
        def ois = new ObjectInputStream(bin)
        return ois.readObject()
    }

}

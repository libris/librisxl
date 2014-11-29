package whelk.plugin

import spock.lang.*


@Unroll
class MarcFrameConverterSpec extends Specification {

    static converter = new MarcFrameConverter() {
        def config
        void initialize(URIMinter uriMinter, Map config) {
            super.initialize(uriMinter, config)
            this.config = config
            super.conversion.doPostProcessing = false
        }
    }

    static fieldSpecs = []
    static marcSkeletons = [:]
    static marcResults = [:]

    static postProcStepSpecs = []

    static {
        ['bib', 'auth', 'hold'].each { marcType ->
            def ruleSets = converter.conversion.marcRuleSets
            converter.config[marcType].each { code, dfn ->
                if (code == 'thingLink')
                    return
                if (code == 'postProcessing') {
                    ruleSets[marcType].postProcSteps.eachWithIndex { step, i ->
                        dfn[i]._spec.each {
                            postProcStepSpecs << [step: step, spec: it]
                        }
                    }
                    return
                }

                if (code == '000') {
                    marcSkeletons[marcType] = dfn._specSource
                    marcResults[marcType] = dfn._specResult
                }
                if (dfn._specSource && dfn._specResult) {
                    fieldSpecs << [source: dfn._specSource,
                                   normalized: dfn._specNormalized,
                                   result: dfn._specResult,
                                   marcType: marcType, code: code]
                } else if (dfn._spec instanceof List) {
                    dfn._spec.each {
                        if (it instanceof Map && it.source && it.result) {
                            fieldSpecs << [source: it.source,
                                           normalized: it.normalized,
                                           result: it.result,
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

    def "should convert field spec for #fieldSpec.marcType #fieldSpec.code"() {
        given:
        def marcType = fieldSpec.marcType
        def marc = deepcopy(marcSkeletons[marcType])
        if (fieldSpec.source instanceof List) {
            marc.fields += fieldSpec.source
        } else if (fieldSpec.source.fields) {
            marc.fields = fieldSpec.source.fields
        } else {
            marc.fields << fieldSpec.source
        }
        when:
        def result = converter.runConvert(marc)
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

    //@Ignore
    def "should revert field spec for #fieldSpec.marcType #fieldSpec.code"() {
        given:
        def marcType = fieldSpec.marcType
        def jsonld = deepcopy(marcResults[marcType])
        fieldSpec.result.each { prop, obj ->
            def value = jsonld[prop]
            if (value instanceof Map) value.putAll(obj)
            else jsonld[prop] = obj
        }
        when:
        def result = converter.conversion.revert(jsonld)
        def expected = deepcopy(marcSkeletons[marcType])
        def source = fieldSpec.normalized ?: fieldSpec.source
        if (source instanceof List) {
            expected.fields += source
        } else if (source.fields) {
            expected.fields = source.fields
        } else {
            expected.fields << source
        }
        then:
        result == expected
        where:
        fieldSpec << fieldSpecs
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
                ["100": ["ind1": "0", "subfields": [["a": "somebody"], ["?": "?"]]]],
                ["100": ["ind2": "0", "subfields": [["a": "somebody"]]]],
                ["024": ["ind1": "9", "subfields": [["a": "123"]]]],
                ["999": "N/A"]
            ]
        ]
        when:
        def frame = converter.runConvert(marc)
        then:
        frame._marcUncompleted == [
            ["008": "020409 | anznnbabn          |EEEEEEEEEEE"],
            ["100": ["ind1": "0", "subfields": [["a": "somebody"], ["?": "?"]]], "_unhandled": ["?"]],
            ["100": ["ind2": "0", "subfields": [["a": "somebody"]]], "_unhandled": ["a"]],
            ["024": ["ind1": "9", "subfields": [["a": "123"]]], "_unhandled": ["ind1"]],
            ["999": "N/A"]
        ]
        frame._marcBroken == [["100": "..."]]
        frame._marcFailedFixedFields == [
            "008": ["38": "E", "39": "E", "29": "E", "30": "E", "31": "E", "34": "E"]
        ]
    }

    def "should handle postprocessing"() {
        when:
        def data = deepcopy(item.spec.source)
        item.step.modify(null, data)
        then:
        data == item.spec.result
        where:
        item << postProcStepSpecs
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

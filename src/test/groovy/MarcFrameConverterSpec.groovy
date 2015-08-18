package whelk.plugin.libris


import whelk.plugin.URIMinter
import spock.lang.*


@Unroll
class MarcFrameConverterSpec extends Specification {

    static converter = new MarcFrameConverter() {
        def config
        void initialize(URIMinter uriMinter, Map config) {
            super.initialize(uriMinter, config)
            this.config = config
            super.conversion.doPostProcessing = false
            super.conversion.flatQuotedForm = false
        }
    }

    static fieldSpecs = []
    static marcSkeletons = [:]
    static marcResults = [:]

    static postProcStepSpecs = []

    static {

        converter.conversion.sharedPostProcSteps.eachWithIndex { step, i ->
            def dfn = converter.config.postProcessing
            dfn[i]._spec.each {
                postProcStepSpecs << [step: step, spec: it]
            }
        }

        ['bib', 'auth', 'hold'].each { marcType ->
            def ruleSets = converter.conversion.marcRuleSets
            converter.config[marcType].each { code, dfn ->
                def ruleSet = ruleSets[marcType]
                def thingLink = ruleSet.thingLink
                if (code == 'thingLink')
                    return
                if (code == 'postProcessing') {
                    ruleSet.postProcSteps.eachWithIndex { step, i ->
                        dfn[i]._spec.each {
                            postProcStepSpecs << [step: step, spec: it, thingLink: thingLink]
                        }
                    }
                    return
                }

                if (code == '000') {
                    marcSkeletons[marcType] = dfn._spec[0].source
                    marcResults[marcType] = dfn._spec[0].result
                }
                if (dfn._spec instanceof List) {
                    dfn._spec.each {
                        if (it instanceof Map && it.source && it.result) {
                            fieldSpecs << [source: it.source,
                                           normalized: it.normalized,
                                           result: it.result,
                                           name: it.name ?: "",
                                           marcType: marcType, code: code,
                                           thingLink: thingLink]
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

    def "should parse formatted #date"() {
        expect:
        MarcSimpleFieldHandler.parseDate(date)
        where:
        date << ['2014-06-10T12:05:05.0+02:00', '2014-06-10T12:05:05.0+0200']
    }

    def "should treat arrays as sets of objects"() {
        given:
        def obj = [:]
        def prop = "label"
        when:
        2.times {
            BaseMarcFieldHandler.addValue(obj, prop, value, true)
        }
        then:
        obj[prop] == [value]
        where:
        value << ["Text", ["@id": "/link"]]
    }

    def "should convert field spec for #fieldSpec.marcType #fieldSpec.code (#fieldSpec.name)"() {
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
        expected[fieldSpec.thingLink]['@id'] = result[fieldSpec.thingLink]['@id']
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
    def "should revert field spec for #fieldSpec.marcType #fieldSpec.code (#fieldSpec.name)"() {
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
            expected.fields.sort { fld -> fld.keySet()[0] }
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

    def "should copy over unhandled marc fields"() {
        given:
        def jsonld = [
            controlNumber: "0000000",
            about: [
                "@type": ["Text", "Monograph"]
            ],
            _marcUncompleted: [
                ["008": "020409 | anznnbabn          |EEEEEEEEEEE"],
                ["100": ["ind1": "0", "subfields": [["?": "?"]]], "_unhandled": ["?"]]
            ]
        ]
        when:
        def result = converter.conversion.revert(jsonld)
        then:
        result.fields == [
            ["001": "0000000"],
            ["008": "020409 | anznnbabn          |EEEEEEEEEEE"],
            ["100": ["ind1": "0", "subfields": [["?": "?"]]]]
        ]
    }

    def "should handle postprocessing"() {
        given:
        def data = deepcopy(item.spec.source)
        def record = data
        def thing = item.thingLink in data? data[item.thingLink] : data

        when:
        item.step.modify(record, thing)
        then:
        data == item.spec.result

        when:
        item.step.unmodify(record, thing)
        then:
        !item.spec.back || data == (item.spec.back == true?
                                    item.spec.source : item.spec.back)

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

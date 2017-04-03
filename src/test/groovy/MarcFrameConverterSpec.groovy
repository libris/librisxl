package whelk.converter.marc


import spock.lang.*


@Unroll
class MarcFrameConverterSpec extends Specification {

    static converter = new MarcFrameConverter() {
        def config
        void initialize(Map config) {
            super.initialize(config)
            this.config = config
            super.conversion.doPostProcessing = false
            super.conversion.flatLinkedForm = false
            super.conversion.baseUri = new URI("/")
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
            converter.config[marcType].each { tag, dfn ->
                def ruleSet = ruleSets[marcType]
                def thingLink = ruleSet.thingLink

                if (tag == 'postProcessing') {
                    ruleSet.postProcSteps.eachWithIndex { step, i ->
                        dfn[i]._spec.each {
                            postProcStepSpecs << [step: step, spec: it, thingLink: thingLink]
                        }
                    }
                    return
                }

                if (tag == '000') {
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
                                           marcType: marcType, tag: tag,
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

    def "should convert field spec for #fieldSpec.marcType #fieldSpec.tag (#fieldSpec.name)"() {
        given:
        def marcType = fieldSpec.marcType
        def marc = fieldSpec.tag == '000'
                ? [leader: fieldSpec.source.leader]
                : deepcopy(marcSkeletons[marcType])
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
        assert expected.containsKey(fieldSpec.thingLink)
        def resultThingId = result[fieldSpec.thingLink]['@id']
        if (resultThingId) {
            expected[fieldSpec.thingLink]['@id'] = resultThingId
        }
        fieldSpec.result.each { prop, obj ->
            def value = expected[prop]
            if (value instanceof Map) value.putAll(obj)
            else expected[prop] = obj
        }
        then:
        assertJsonEquals(result, expected)
        where:
        fieldSpec << fieldSpecs
    }

    @Requires({ env.mfspec == 'all' })
    def "should revert field spec for #fieldSpec.marcType #fieldSpec.tag (#fieldSpec.name)"() {
        given:
        def marcType = fieldSpec.marcType
        def jsonld = deepcopy(marcResults[marcType])
        fieldSpec.result.each { prop, obj ->
            def value = jsonld[prop]
            if (value instanceof Map) value.putAll(obj)
            else jsonld[prop] = obj
        }
        expect:
        converter.conversion.getRuleSetFromJsonLd(jsonld).name == marcType
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
        assertJsonEquals(result, expected)
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
            "mainEntity": [
                "@type": "Instance",
                "instanceOf": ["@type": "Text"]
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

    def "should process extra data"() {
        given:
        def conv = converter.conversion
        def thing = [:]
        def entityMap = ['?thing': thing]
        def extraData = ['oaipmhSetSpecs': ['bibid:123', 'location:S']]
        when:
        conv.marcRuleSets['hold'].processExtraData(entityMap, extraData)
        then:
        thing['heldBy']['@id'] == 'https://libris.kb.se/library/S'
        thing['holdingFor']['@id'] == 'http://libris.kb.se/resource/bib/123'
    }

    def completeEntities = converter.conversion.marcRuleSets['auth'].&completeEntities
    String r(path) { new URI("http://libris.kb.se/").resolve(path) }
    def link(v) { ['@id': r(v)] }

    def "should make ids"() {
        given:
        def record = ['@id': null, controlNumber: "123"]
        def thing = ['@id': null]
        when:
        completeEntities(['?record': record, '?thing': thing])
        then:
        record['@id'] == r('auth/123')
        thing['@id'] == r('resource/auth/123')
    }

    def "should use record id"() {
        given:
        def record = ['@id': '/fnrblgr', controlNumber: "123"]
        def thing = ['@id': null]
        when:
        completeEntities(['?record': record, '?thing': thing])
        then:
        record['@id'] == '/fnrblgr'
        record['sameAs'] == [link('auth/123')]
        thing['@id'] == '/fnrblgr#it'
        thing['sameAs'] == [link('resource/auth/123')]
    }

    def "should use thing id"() {
        given:
        def record = ['@id': null, controlNumber: "123"]
        def thing = ['@id': '/thing']
        when:
        completeEntities(['?record': record, '?thing': thing])
        then:
        record['@id'] == r('auth/123')
        thing['@id'] == '/thing'
        thing['sameAs'] == [link('resource/auth/123')]
    }

    def "should use record and thing id"() {
        given:
        def record = ['@id': '/fnrblgr', controlNumber: "123"]
        def thing = ['@id': '/thing']
        when:
        completeEntities(['?record': record, '?thing': thing])
        then:
        record['@id'] == '/fnrblgr'
        record['sameAs'] == [link('auth/123')]
        thing['@id'] == '/thing'
        thing['sameAs'] == [link('resource/auth/123')]
    }

    def "should handle uri:s without dollar signs"(){
        given:
        def data
        def dataWithOutDollars
        def converter
        when:
        converter = new MarcFrameConverter()
        dataWithOutDollars = ['doc':['leader':'01103cam a2200265 a 4500', 'fields':[['001':'9387233'], ['005':'20101001124525.0'], ['008':'040302s2002    sw ||||      |10| 0 swe c'], ['020':['ind1':' ', 'ind2':' ', 'subfields':[['a':'91-89655-25-7']]]], ['040':['ind1':' ', 'ind2':' ', 'subfields':[['a':'Ai']]]], ['042':['ind1':' ', 'ind2':' ', 'subfields':[['9':'ARB']]]], ['100':['ind1':'1', 'ind2':' ', 'subfields':[['a':'Essén, Anna,'], ['d':'1977-']]]], ['245':['ind1':'0', 'ind2':'0', 'subfields':[['a':'Svensk invandring och arbetsmarknaden :'], ['b':'återblick och nuläge /'], ['c':'Essén,  Anna']]]], ['260':['ind1':' ', 'ind2':' ', 'subfields':[['a':'Stockholm :'], ['b':'Institutet för framtidsstudier,'], ['c':'2002']]]], ['300':['ind1':' ', 'ind2':' ', 'subfields':[['a':'64 s.']]]], ['440':['ind1':' ', 'ind2':'0', 'subfields':[['a':'Arbetsrapport / Institutet för Framtidsstudier,'], ['x':'1652-120X ;'], ['v':'2002:6']]]], ['650':['ind1':' ', 'ind2':'7', 'subfields':[['a':'Invandrare'], ['2':'albt//swe']]]], ['650':['ind1':' ', 'ind2':'7', 'subfields':[['a':'Arbetsmarknad'], ['2':'albt//swe']]]], ['650':['ind1':' ', 'ind2':'7', 'subfields':[['a':'Sysselsättningsmöjligheter'], ['2':'albt//swe']]]], ['650':['ind1':' ', 'ind2':'7', 'subfields':[['a':'Trender och tendenser'], ['2':'albt//swe']]]], ['650':['ind1':' ', 'ind2':'7', 'subfields':[['a':'Arbetsmarknadsstatistik'], ['2':'albt//swe']]]], ['650':['ind1':' ', 'ind2':'7', 'subfields':[['a':'Sverige'], ['2':'albt//swe']]]], ['710':['ind1':'2', 'ind2':' ', 'subfields':[['a':'Institutet för framtidsstudier'], ['4':'pbl']]]], ['776':['ind1':'0', 'ind2':'8', 'subfields':[['i':'Online'], ['a':'Essén, Anna, 1977-'], ['t':'Svensk invandring och arbetsmarknaden'], ['d':'2002'], ['w':'10149216']]]], ['856':['ind1':'4', 'ind2':'1', 'subfields':[['u':'http://www.framtidsstudier.se/filebank/files/20051201133251fil048Ti3PL2UIwRJQEBbDG.pdf'], ['z':'Fritt tillgänglig via Institutet för framtidsstudier']]]]]], 'id':'fxqp0f0r1nfrgss', 'spec':null]

        //Does not blow
        def resultWithoutDollars = converter.convert(dataWithOutDollars.doc, dataWithOutDollars.id)

        then:
        resultWithoutDollars != null
    }

    def "should get as list"() {
        expect:
        Util.asList('1') == ['1']
        Util.asList(['1']) == ['1']
        Util.asList('') == ['']
        Util.asList(null) == []
    }

    def "should get by path"() {
        expect:
        Util.getAllByPath(entity, path) == values
        where:
        entity                          | path              | values
        [key: '1']                      | 'key'             | ['1']
        [key: ['1', '2']]               | 'key'             | ['1', '2']
        [item: [[key: '1']]]            | 'item.key'        | ['1']
        [item: [[key: '1'],
                [key: '2']]]            | 'item.key'        | ['1', '2']
        [part: [[item: [[key: '1']]],
                [item: [key: '2']]]]    | 'part.item.key'   | ['1', '2']
    }

    def "should process includes"() {
        expect:
        MarcRuleSet.processInclude([patterns: [a: [a:1]]], [include: 'a', b:2]) == [a:1, b:2]
    }

    void assertJsonEquals(result, expected) {
        def resultJson = json(result)
        def expectedJson = json(expected)
        assert resultJson == expectedJson
    }

    private deepcopy(orig) {
        def bos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(bos)
        oos.writeObject(orig); oos.flush()
        def bin = new ByteArrayInputStream(bos.toByteArray())
        def ois = new ObjectInputStream(bin)
        return ois.readObject()
    }

    private json(obj) {
        return converter.mapper.defaultPrettyPrintingWriter().writeValueAsString(
                sorted(obj))
    }

    def sorted(obj) {
        if (obj instanceof Map) {
            TreeMap sortedmap = new TreeMap()
            obj.each { k, v ->
                sortedmap[k] = (v instanceof List)?
                    v.collect { sorted(it) } : sorted(v)
            }
            return sortedmap
        }
        return obj
    }

}

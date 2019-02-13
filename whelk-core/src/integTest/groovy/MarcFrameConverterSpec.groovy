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

        MarcFrameCli.addJsonLd(converter)

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
                    dfn._spec.eachWithIndex { it, i ->
                        if (it instanceof Map &&
                            (it.source || it.normalized) &&
                            it.result) {
                            fieldSpecs << [source: it.source,
                                           normalized: it.normalized,
                                           result: it.result,
                                           addOnRevert: it.addOnRevert,
                                           name: it.name ?: "#${i}",
                                           i: i,
                                           marcType: marcType, tag: tag,
                                           thingLink: thingLink]
                        }
                    }
                }
            }
        }
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

    def "should convert field spec for #fieldSpec.marcType #fieldSpec.tag (#fieldSpec.name) [#fieldSpec.i]"() {
        given:
        def marcType = fieldSpec.marcType
        def marc = deepcopy(marcSkeletons[marcType])
        if (fieldSpec.source instanceof List) {
            marc.fields += fieldSpec.source
        } else if (fieldSpec.source.fields) {
            if (fieldSpec.source.leader) {
                marc.leader = fieldSpec.source.leader
            }
            marc.fields = fieldSpec.source.fields
        } else {
            marc.fields << fieldSpec.source
        }
        when:
        def result = converter.runConvert(marc)
        def expected = deepcopy(marcResults[marcType])

        // test id generation separately
        if (result['@id']) {
            expected['@id'] = result['@id']
        } else {
            expected.remove('@id')
        }
        assert expected.containsKey(fieldSpec.thingLink)
        def resultThingId = result[fieldSpec.thingLink]['@id']
        if (resultThingId) {
            expected[fieldSpec.thingLink]['@id'] = resultThingId
        } else {
            expected[fieldSpec.thingLink].remove('@id')
        }

        fieldSpec.result.each { prop, obj ->
            def value = expected[prop]
            if (value instanceof Map) value.putAll(obj)
            else expected[prop] = obj
        }
        then:
        assertJsonEquals(result, expected)
        where:
        fieldSpec << fieldSpecs.findAll { it.source }
    }

    @Requires({ env.mfspec == 'all' })
    def "should revert field spec for #fieldSpec.marcType #fieldSpec.tag (#fieldSpec.name) [#fieldSpec.i]"() {
        given:
        def marcType = fieldSpec.marcType
        def jsonld = deepcopy(marcResults[marcType])
        [fieldSpec.result, fieldSpec.addOnRevert].each {
            it.each { prop, obj ->
                def value = jsonld[prop]
                if (value instanceof Map) value.putAll(obj)
                else jsonld[prop] = obj
            }
        }
        expect:
        converter.conversion.getRuleSetFromJsonLd(jsonld).name == marcType
        when:
        def result = converter.conversion.revert(jsonld)

        def source = fieldSpec.normalized ?: fieldSpec.source

        def expected = fieldSpec.tag == '000'
                ? [leader: source.leader]
                : deepcopy(marcSkeletons[marcType])

        if (source instanceof List) {
            expected.fields += source
            expected.fields.sort { fld -> fld.keySet()[0] }
        } else if (source.fields) {
            expected.fields = source.fields
        } else {
            expected.fields << source
        }
        then:
        assertJsonEqualsOnRevert(result, expected)
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
        // NOTE: unhandled fixed field columns now stored as code strings
        frame['marc:modifiedRecord']['code'] == 'E'
        frame._marcUncompleted == [
            //["008": "020409 | anznnbabn          |EEEEEEEEEEE"],
            ["100": ["ind1": "0", "subfields": [["a": "somebody"], ["?": "?"]]], "_unhandled": ["?"]],
            ["024": ["ind1": "9", "subfields": [["a": "123"]]], "_unhandled": ["ind1"]],
            ["999": "N/A"]
        ]
        frame._marcBroken == [["100": "..."]]
        //frame._marcFailedFixedFields == [
        //    "008": ["38": "E", "39": "E", "29": "E", "30": "E", "31": "E", "34": "E"]
        //]
    }

    def "should copy over unhandled marc fields"() {
        given:
        def jsonld = [
            controlNumber: "0000000",
            "mainEntity": [
                "@type": "Instance",
                "instanceOf": [
                    "@type": "Text",
                    "contribution": [
                        ["@type": "PrimaryContribution",
                         "agent": ["@type": "Person", "name": "X"]]
                    ]
                ]
            ],
            _marcUncompleted: [
                ["008": "020409 | anznnbabn          |EEEEEEEEEEE"],
                ["100": ["ind1": "0", "subfields": [["a": "X"]]], "_unhandled": ["?"]]
            ]
        ]
        when:
        def result = converter.conversion.revert(jsonld)
        then:
        result.fields == [
            ["001": "0000000"],
            ["100": ["ind1": "0", "ind2": " ", "subfields": [["a": "X"]]]],
            ["008": "020409 | anznnbabn          |EEEEEEEEEEE"]
        ]

    }

    def "should accept sameAs-token-URIs on revert"() {
        given:
        def jsonld = [
            controlNumber: "0000000",
            "created": "1990-01-01T00:00:00.0+01:00",
            "mainEntity": [
                "@type": "Instance",
                "issuanceType": "Monograph",
                "instanceOf": [
                    "@type": "Text",
                    "language": [["@id": "https://id.kb.se/language/swe"]],
                    "genreForm": [
                        ["sameAs": [["@id": "https://id.kb.se/marc/BooksLiteraryFormType-1"]]],
                        ["sameAs": [["@id": "https://id.kb.se/marc/BooksBiographyType-d"]]]
                    ]
                ]
            ]
        ]
        when:
        def result = converter.conversion.revert(jsonld)
        then:
        result.fields[1]["008"] == "900101|        |  |||||||||||000 1dswe| "
    }

    def "should handle postprocessing on convert"() {
        given:
        def data = deepcopy(item.spec.source)
        def record = data
        def thing = item.thingLink in data? data[item.thingLink] : data
        def expected = getSpecPiece(item.spec, 'result')

        when:
        item.step.modify(record, thing)
        then:
        assertJsonEquals(data, expected)

        where:
        item << postProcStepSpecs.findAll { it.spec.source }
    }

    def "should handle postprocessing on revert"() {
        given:
        def data = getSpecPiece(item.spec, 'result')
        def record = data
        def thing = item.thingLink in data? data[item.thingLink] : data

        when:
        item.step.unmodify(record, thing)
        then:
        if (item.spec.back) {
            def expected = getSpecPiece(item.spec, 'back')
            assertJsonEquals(data, expected)
        }

        where:
        item << postProcStepSpecs.findAll { it.spec.source }
    }

    private getSpecPiece(spec, pieceKey) {
        def piece = spec[pieceKey]
        if (piece instanceof String) {
            pieceKey = piece
        }
        if (pieceKey == "both") {
            return deepcopy(spec.source + spec.result)
        } else {
            return deepcopy(spec[pieceKey])
        }
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
        thing['itemOf']['@id'] == 'http://libris.kb.se/resource/bib/123'
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

    void assertJsonEquals(result, expected) {
        def resultJson = json(result)
        def expectedJson = json(expected)
        assert resultJson == expectedJson
    }

    void assertJsonEqualsOnRevert(result, expected) {
        def resultJson = json(result)
        def expectedJson = json(expected)

        // If unequal, first check without indicators and subfield order,
        // then check the extracted shape of the latter.
        if (resultJson != expectedJson) {

            def normResultJson = deepcopy(result)
            def resultSubfieldShapes = extractSubfieldShape(normResultJson)

            def normExpectedJson = deepcopy(expected)
            def expectedSubfieldShapes = extractSubfieldShape(normExpectedJson)

            if (normResultJson == normExpectedJson) {
                // should fail since norm-sorted check passed
                def resultSubfieldShapesJson = json(resultSubfieldShapes)
                def expectedSubfieldShapesJson = json(expectedSubfieldShapes)
                assert resultSubfieldShapesJson == expectedSubfieldShapesJson
            }
        }

        assert resultJson == expectedJson
    }

    Map extractSubfieldShape(Map record) {
        Map subfieldShapes = [:]
        if (record instanceof Map) {
            for (field in record.fields) {
                field.each { tag, fieldData ->
                    if (fieldData instanceof Map) {
                        // store shape
                        def fieldShape = [
                            ind1: fieldData.ind1,
                            ind2: fieldData.ind2,
                            subfields: fieldData.subfields.collect { it.keySet()[0] }
                        ]
                        subfieldShapes.get(tag, []) << fieldShape
                        // erase extracted shape
                        fieldData.remove('ind1')
                        fieldData.remove('ind2')
                        fieldData.subfields.sort { it.keySet()[0] }
                    }
                }
            }
        }
        return subfieldShapes
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
            obj.keySet().sort().each { k ->
                def v = obj[k]
                sortedmap[k] = sorted(v)
            }
            return sortedmap
        } else if (obj instanceof List) {
            return obj.collect { sorted(it) }
        }
        return obj
    }

}

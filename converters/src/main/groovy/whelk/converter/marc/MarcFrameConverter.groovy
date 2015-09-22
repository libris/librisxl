package whelk.converter.marc

import groovy.util.logging.Slf4j as Log

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.regex.Pattern

import org.codehaus.jackson.map.ObjectMapper

import whelk.Document

import whelk.converter.FormatConverter
import whelk.converter.MarcJSONConverter
import whelk.converter.URIMinter

import com.damnhandy.uri.template.UriTemplate
import com.damnhandy.uri.template.UriUtil


@Log
class MarcFrameConverter implements FormatConverter {

    URIMinter uriMinter
    ObjectMapper mapper = new ObjectMapper()

    protected MarcConversion conversion

    MarcFrameConverter(uriSpacePath="ext/oldspace.json") {
        def loader = getClass().classLoader
        loader.getResourceAsStream(uriSpacePath).withStream {
            uriMinter = new URIMinter(mapper.readValue(it, Map))
        }
        def config = loader.getResourceAsStream("ext/marcframe.json").withStream {
            mapper.readValue(it, Map)
        }
        initialize(uriMinter, config)
    }

    MarcFrameConverter(URIMinter uriMinter, Map config) {
        initialize(uriMinter, config)
    }

    void initialize(URIMinter uriMinter, Map config) {
        def tokenMaps = [:]
        def loader = getClass().classLoader
        config.tokenMaps.each { key, sourceRef ->
            if (sourceRef instanceof String) {
                tokenMaps[key] = loader.getResourceAsStream("ext/"+sourceRef).withStream {
                    mapper.readValue(it, List).collectEntries { [it.code, it] }
                }
            } else {
                tokenMaps[key] = sourceRef
            }
        }
        conversion = new MarcConversion(config, uriMinter, tokenMaps)
    }

    Map runConvert(Map marcSource, Map extraData=null) {
        return conversion.convert(marcSource, null, extraData)
    }

    Map runRevert(Map data) {
        return conversion.revert(data)
    }

    @Override
    String getResultContentType() { "application/ld+json" }

    @Override
    String getRequiredContentType() { "application/x-marc-json" }

    Document doConvert(final Object record, final Map metaentry) {
        try {
            def source = MarcJSONConverter.toJSONMap(record)
            def result = runConvert(source, metaentry?.extraData)
            log.trace("Created frame: $result")

            return new Document(result, metaentry).withContentType(getResultContentType())
        } catch (Exception e) {
            log.error("Failed marc conversion (${e.message}). Metaentry: $metaentry")
            throw e
        }
    }

    @Override
    Document convert(final Document doc) {
        def source = doc.data
        def meta = doc.manifest?.extraData
        def result = runConvert(source, meta)
        log.trace("Created frame: $result")

        return new Document(result, doc.manifest).withIdentifier(((String)doc.identifier)).withContentType(getResultContentType())
    }

    public static void main(String[] args) {
        def fpath = args[0]
        def cmd = null
        def uriSpace = null
        if (args.length > 2) {
            cmd = args[0]
            uriSpace = args[1]
            fpath = args[2]
        } else if (args.length > 1) {
            cmd = args[0]
            fpath = args[1]
        }
        def converter = uriSpace? new MarcFrameConverter(uriSpace) : new MarcFrameConverter()
        def source = converter.mapper.readValue(new File(fpath), Map)
        def result = null
        if (cmd == "revert") {
            result = converter.runRevert(source)
        } else {
            result = converter.runConvert(source)
        }
        converter.mapper.writeValue(System.out, result)
    }

}


class MarcConversion {

    static MARC_CATEGORIES = ['bib', 'auth', 'hold']

    List<MarcFramePostProcStep> sharedPostProcSteps
    Map<String, MarcRuleSet> marcRuleSets = [:]
    boolean doPostProcessing = true
    boolean flatQuotedForm = true
    Map marcTypeMap = [:]
    Map tokenMaps

    URIMinter uriMinter

    MarcConversion(Map config, URIMinter uriMinter, Map tokenMaps) {
        marcTypeMap = config.marcTypeFromTypeOfRecord
        this.uriMinter = uriMinter
        this.tokenMaps = tokenMaps

        this.sharedPostProcSteps = config.postProcessing.collect {
            parsePostProcStep(it)
        }

        MARC_CATEGORIES.each { marcCat ->
            def marcRuleSet = new MarcRuleSet(this, marcCat)
            marcRuleSets[marcCat] = marcRuleSet
            marcRuleSet.buildHandlers(config)
        }
        addTypeMaps()
    }

    MarcFramePostProcStep parsePostProcStep(Map stepDfn) {
        def props = stepDfn.clone()
        for (k in stepDfn.keySet())
            if (k[0] == '_')
                props.remove(k)
        switch (stepDfn.type) {
            case 'FoldLinkedProperty': new FoldLinkedPropertyStep(props); break
            case 'FoldJoinedProperties': new FoldJoinedPropertiesStep(props); break
            case 'SetUpdatedStatus': new SetUpdatedStatusStep(props); break
            case 'MappedProperty': new MappedPropertyStep(props); break
        }
    }

    void addTypeMaps() {
        tokenMaps.typeOfRecord.each { token, typeName ->
            def marcCat = marcTypeMap[token] ?: marcTypeMap['*']
            def marcRuleSet = marcRuleSets[marcCat] 
            marcRuleSet.aboutTypeMap['?thing'] << typeName
        }
    }

    String getMarcCategory(String leader) {
        def typeOfRecord = getTypeOfRecord(leader)
        return marcTypeMap[typeOfRecord] ?: marcTypeMap['*']
    }

    String getTypeOfRecord(String leader) {
        return leader.substring(6, 7)
    }

    String getBibLevel(String leader) {
        return leader.substring(7, 8)
    }

    MarcRuleSet getRuleSetFromJsonLd(Map data) {
        def selected = null
        for (ruleSet in marcRuleSets.values()) {
            if (ruleSet.matchesData(data)) {
                selected = ruleSet
                break
            }
        }
        return selected ?: marcRuleSets['bib']
    }

    Map convert(Map marcSource, String recordId=null, Map extraData=null) {

        def leader = marcSource.leader
        def marcCat = getMarcCategory(leader)
        def marcRuleSet = marcRuleSets[marcCat]

        def record = ["@id": recordId]
        def thing = [:]

        def marcRemains = [failedFixedFields: [:], uncompleted: [], broken: []]

        def state = [
            sourceMap: [leader: leader],
            bnodeCounter: 0,
            entityMap: ['?record': record, '?thing': thing],
            quotedEntities: [],
            marcRemains: marcRemains
        ]

        marcRuleSet.convert(marcSource, state)

        if (marcRemains.uncompleted.size() > 0) {
            record._marcUncompleted = marcRemains.uncompleted
        }
        if (marcRemains.broken.size() > 0) {
            record._marcBroken = marcRemains.broken
        }
        if (marcRemains.failedFixedFields.size() > 0) {
            record._marcFailedFixedFields = marcRemains.failedFixedFields
        }

        if (doPostProcessing) {
            sharedPostProcSteps.each {
                it.modify(record, thing)
            }
            marcRuleSet.postProcSteps.each {
                it.modify(record, thing)
            }
        }

        // TODO: make this configurable
        if (record['@id'] == null) {
            def uriMap = uriMinter.computePaths(record, marcCat)
            record['@id'] = uriMap['document']
            thing['@id'] = uriMap['thing']
        }

        // TODO: move this to an "extra data" section in marcframe.json?
        extraData?.get("oaipmhSetSpecs")?.each {
            if (marcCat == "hold") {
                def prefix = "bibid:"
                if (it.startsWith(prefix) && !thing.holdingFor) {
                    thing.holdingFor = ["@type": "Record",
                                        controlNumber: it.substring(prefix.size())]
                }
                prefix = "location:"
                if (it.startsWith(prefix) && !thing.heldBy) {
                    thing.heldBy = ["@type": "Organization",
                                    notation: it.substring(prefix.size())]
                }
            }
        }

        def linkedThing = record[marcRuleSet.thingLink]
        if (linkedThing) {
            thing.each { key, value ->
                def hasValue = linkedThing[key]
                if (hasValue) {
                    if (!(hasValue instanceof List)) {
                        hasValue = [hasValue]
                    }
                    hasValue += value
                } else {
                    linkedThing[key] = value
                }
            }
        } else {
            record[marcRuleSet.thingLink] = thing
        }

        if (flatQuotedForm) {
            return toFlatQuotedForm(state, marcRuleSet.thingLink)
        } else {
            return record
        }
    }

    def toFlatQuotedForm(state, thingLink) {
        def record = state.entityMap['?record']
        def thing = record[thingLink]
        def thingId = thing['@id']
        if (thingId) {
            record[thingLink] = ['@id': thingId]
        }

        def quotedEntities = []
        def quotedIds = new HashSet()

        state.quotedEntities.each { ent ->
            def entId = ent['@id'] ?: '_:' + createKeyString(ent)
            if (entId) {
                def copy = ent.clone()
                ent.clear()
                ent['@id'] = copy['@id'] = entId
                if (copy.size() > 1 && !quotedIds.contains(entId)) {
                    quotedIds << entId
                    quotedEntities << ['@graph': copy]
                }
            }
        }

        return [
            'descriptions': [entry: record, items: [thing], quoted: quotedEntities]
        ]
    }

    String createKeyString(ent) {
        if (ent instanceof String) {
            return UriUtil.encode(ent.trim())
        }
        return ent.keySet().toList().sort().collect {
            def v = ent[it]
            return v instanceof List? v.collect { createKeyString(it) } :
                    createKeyString(v)
        }.join(':').toLowerCase()
    }

    Map revert(data) {
        def marcRuleSet = getRuleSetFromJsonLd(data)

        if (doPostProcessing) {
            sharedPostProcSteps.each {
                it.unmodify(data, data[marcRuleSet.thingLink])
            }
            marcRuleSet.postProcSteps.each {
                it.unmodify(data, data[marcRuleSet.thingLink])
            }
        }

        def marc = [:]
        def fields = []
        marc['fields'] = fields
        marcRuleSet.fieldHandlers.each { tag, handler ->
            def value = handler.revert(data, marc)
            if (tag == "000") {
                marc.leader = value
            } else {
                if (value == null)
                    return
                if (value instanceof List) {
                    value.each {
                        if (it) {
                            fields << [(tag): it]
                        }
                    }
                } else {
                    fields << [(tag): value]
                }
            }
        }
        // TODO: how to re-add only partially _unhandled?
        data._marcUncompleted.each {
            def field = it.clone()
            field.remove('_unhandled')
            fields << field
        }
        return marc
    }

}

class MarcRuleSet {

    static PREPROC_TAGS = ["000", "001", "006", "007", "008"] as Set

    MarcConversion conversion
    String name

    String thingLink
    def fieldHandlers = [:]
    List<MarcFramePostProcStep> postProcSteps

    Set primaryTags = new HashSet()
    Map<String, Set<String>> aboutTypeMap = new HashMap<String, Set<String>>()

    MarcRuleSet(conversion, name) {
        this.conversion = conversion
        this.name = name
        this.aboutTypeMap['?record'] = new HashSet<String>()
        this.aboutTypeMap['?thing'] = new HashSet<String>()
    }

    void buildHandlers(config) {
        def subConf = config[name]

        subConf.each { tag, dfn ->

            if (tag == 'thingLink') {
                thingLink = dfn
                return
            } else if (tag == 'postProcessing') {
                postProcSteps = dfn.collect {
                    conversion.parsePostProcStep(it)
                }
                return
            }

            if (dfn.inherit) {
                dfn = processInherit(config, subConf, tag, dfn)
            }
            if (dfn.ignored || dfn.size() == 0) {
                return
            }
            def handler = null
            if (dfn.tokenTypeMap) {
                handler = new TokenSwitchFieldHandler(this, tag, dfn)
            } else if (dfn.recTypeBibLevelMap) {
                handler = new TokenSwitchFieldHandler(this, tag, dfn, 'recTypeBibLevelMap')
            } else if (dfn.find { it.key[0] == '[' }) {
                handler = new MarcFixedFieldHandler(this, tag, dfn)
            } else if (dfn.find { it.key[0] == '$' }) {
                handler = new MarcFieldHandler(this, tag, dfn)
                if (handler.dependsOn) {
                    primaryTags += handler.dependsOn
                }
                if (handler.definesDomainEntityType != null) {
                    primaryTags << tag
                }
            }
            else {
                handler = new MarcSimpleFieldHandler(this, tag, dfn)
                assert handler.property || handler.uriTemplate, "Incomplete: $tag: $dfn"
            }
            fieldHandlers[tag] = handler
            if (dfn.aboutType) {
                aboutTypeMap[dfn.aboutEntity ?: '?thing'] << dfn.aboutType
            }
        }
    }

    def processInherit(config, subConf, tag, fieldDfn) {
        def ref = fieldDfn.inherit
        def refTag = tag
        if (ref.contains(':')) {
            (ref, refTag) = ref.split(':')
        }
        def baseDfn = (ref in subConf)? subConf[ref] : config[ref][refTag]
        if (baseDfn.inherit) {
            subConf = (ref in config)? config[ref] : subConf
            baseDfn = processInherit(config, subConf, ref ?: refTag, baseDfn)
        }
        def merged = baseDfn + fieldDfn
        merged.remove('inherit')
        return merged
    }

    boolean matchesData(Map data) {
        def thing = data[thingLink]
        if (!thing)
            return false
        def types = thing['@type']
        if (types instanceof String)
            types = [types]
        def aboutTypes = aboutTypeMap['?thing']
        for (type in types) {
            if (type in aboutTypes)
                return true
        }
        return false
    }

    void convert(Map marcSource, Map state) {
        def preprocFields = []
        def primaryFields = []
        def otherFields = []

        marcSource.fields.each { field ->
            field.each { tag, value ->
                def fieldsByTag = state.sourceMap[tag]
                if (fieldsByTag == null) {
                    fieldsByTag = state.sourceMap[tag] = []
                }
                fieldsByTag << field

                if (tag in PREPROC_TAGS) {
                    preprocFields << field
                } else if (tag in primaryTags) {
                    primaryFields << field
                } else {
                    otherFields << field
                }
            }
        }

        fieldHandlers["000"].convert(state, state.sourceMap.leader)
        processFields(state, fieldHandlers, preprocFields)
        processFields(state, fieldHandlers, primaryFields)
        processFields(state, fieldHandlers, otherFields)
    }

    void processFields(state, fieldHandlers, fields) {
        fields.each { field ->
            try {
                def result = null
                field.each { tag, value ->
                    def handler = fieldHandlers[tag]
                    if (handler) {
                        result = handler.convert(state, value)
                    }
                }
                if (!result || !result.ok) {
                    field = field.clone()
                    if (result && result.unhandled) {
                        field['_unhandled'] = result.unhandled as List
                    }
                    state.marcRemains.uncompleted << field
                }
            } catch (MalformedFieldValueException e) {
                state.marcRemains.broken << field
            }
        }
    }

}


class ConversionPart {

    MarcRuleSet ruleSet
    String aboutEntityName
    Map tokenMap
    Map reverseTokenMap
    boolean embedded = false

    void setTokenMap(fieldHandler, dfn) {
        def tokenMap = dfn.tokenMap
        if (tokenMap) {
            reverseTokenMap = [:]
            this.tokenMap = (tokenMap instanceof String)?
                fieldHandler.tokenMaps[tokenMap] : tokenMap
            this.tokenMap.each { k, v ->
                if (v != null) {
                    reverseTokenMap[v] = k
                }
            }
        }
    }

    Map getEntity(Map data) {
        if (aboutEntityName == '?record') {
            return data
        }
        if (ruleSet.thingLink in data) {
            return data[ruleSet.thingLink]
        }
        return data
    }

    def revertObject(obj) {
        if (reverseTokenMap) {
            if (obj instanceof List) {
                return obj.collect { reverseTokenMap[it] }
            } else {
                return reverseTokenMap[obj]
            }
        } else {
            return obj
        }
    }

    Map newEntity(state, type, id=null) {
        def ent = [:]
        if (type) ent["@type"] = type
        if (id) ent["@id"] = id
        if (!embedded) {
            state.quotedEntities << ent
        }
        return ent
    }

    static void addValue(obj, key, value, repeatable) {
        def current = obj[key]
        if (current || repeatable) {
            def l = current ?: []

            def vId = value instanceof Map? value["@id"] : null
            if (vId) {
                def existing = l.find { it instanceof Map && it["@id"] == vId }
                if (existing) {
                    value.putAll(existing)
                    l[l.indexOf(existing)] = value
                    return
                }
            } else if (l.find { it == value }) {
                return
            }

            l << value
            value = l
        }
        obj[key] = value
    }

}

abstract class BaseMarcFieldHandler extends ConversionPart {

    String tag
    Map tokenMaps
    String definesDomainEntityType
    String link
    boolean repeatable = false
    String resourceType

    static final ConvertResult OK = new ConvertResult(true)
    static final ConvertResult FAIL = new ConvertResult(false)

    BaseMarcFieldHandler(ruleSet, tag, fieldDfn) {
        this.ruleSet = ruleSet
        this.tag = tag
        this.tokenMaps = ruleSet.conversion.tokenMaps
        if (fieldDfn.aboutType) {
            definesDomainEntityType = fieldDfn.aboutType
        }
        aboutEntityName = fieldDfn.aboutEntity ?: '?thing'
        if (fieldDfn.addLink) {
            link = fieldDfn.addLink
            repeatable = true
        } else {
            link = fieldDfn.link
        }
        embedded = fieldDfn.embedded == true
        resourceType = fieldDfn.resourceType
    }

    abstract ConvertResult convert(state, value)

    abstract def revert(Map data, Map result)

}

class ConvertResult {
    boolean ok
    Set unhandled = Collections.emptySet()
    ConvertResult(boolean ok) {
        this.ok = ok
    }
    ConvertResult(Set unhandled) {
        this.ok = unhandled.size() == 0
        this.unhandled = unhandled
    }
}

class MarcFixedFieldHandler {

    String tag
    static final String FIXED_UNDEF = "|"
    static final String FIXED_NONE = " "
    static final String FIXED_FAUX_NONE = "_"
    def columns = []
    int fieldSize = 0

    MarcFixedFieldHandler(ruleSet, tag, fieldDfn) {
        this.tag = tag
        fieldDfn.each { key, obj ->
            def m = (key =~ /^\[(\d+):(\d+)\]$/)
            if (m && obj) {
                def start = m[0][1].toInteger()
                def end = m[0][2].toInteger()
                columns << new Column(ruleSet, obj, start, end,
                    obj['fixedDefault'],
                    obj['matchAsDefault'])
                if (end > fieldSize) {
                    fieldSize = end
                }
            }
        }
        columns.sort { it.start }
    }

    ConvertResult convert(state, value) {
        def success = true
        def failedFixedFields = state.marcRemains.failedFixedFields
        for (col in columns) {
            if (!col.convert(state, value).ok) {
                success = false
                def unmapped = failedFixedFields[tag]
                if (unmapped == null) {
                    unmapped = failedFixedFields[tag] = [:]
                }
                def key = "${col.start}"
                if (col.end && col.end != col.start + 1) {
                    key += "_${col.end - col.start}"
                }
                unmapped[key] = col.getToken(value)
            }
        }
        return new ConvertResult(success)
    }

    def revert(Map data, Map result, boolean keepEmpty=false) {
        def value = new StringBuilder(FIXED_NONE * fieldSize)
        def actualValue = false
        for (col in columns) {
            def obj = col.revert(data)
            // TODO: ambiguity trouble if this is a List!
            if (obj instanceof List) {
                obj = obj.find { it }
            }
            if (obj) {
                assert col.width - obj.size() > -1
                assert value.size() > col.start
                assert col.width >= obj.size()
                def end = col.start + obj.size() - 1
                value[col.start .. end] = obj
                if (col.isActualValue(obj)) {
                    actualValue = true
                }
            }
        }
        return (actualValue || keepEmpty)? value.toString() : null
    }

    class Column extends MarcSimpleFieldHandler {
        int start
        int end
        String fixedDefault
        Pattern matchAsDefault
        Column(ruleSet, fieldDfn, start, end, fixedDefault, matchAsDefault=null) {
            super(ruleSet, null, fieldDfn)
            assert start > -1 && end >= start
            this.start = start
            this.end = end
            this.fixedDefault = fixedDefault
            if (matchAsDefault) {
                this.matchAsDefault = Pattern.compile(matchAsDefault)
            }
            if (!fixedDefault && tokenMap &&
                !tokenMap.containsKey(FIXED_FAUX_NONE) &&
                !tokenMap.containsKey(FIXED_NONE)) {
                this.fixedDefault = FIXED_UNDEF
            }
        }
        int getWidth() { return end - start }
        String getToken(value) {
            if (value.size() < start)
                return ""
            if (value.size() < end)
                return value.substring(start)
            return value.substring(start, end)
        }
        ConvertResult convert(state, value) {
            def token = getToken(value)
            if (token == "")
                return OK
            if (token == fixedDefault)
                return OK
            if (matchAsDefault && matchAsDefault.matcher(token).matches())
                return OK
            boolean isNothing = token.find { it != FIXED_NONE && it != FIXED_UNDEF } == null
            if (isNothing)
                return OK
            return super.convert(state, token)
        }
        def revert(Map data) {
            def v = super.revert(data, null)
            if ((v == null || v == [null]) && fixedDefault)
                return fixedDefault
            return v
        }
        boolean isActualValue(String value) {
            return value != fixedDefault &&
                (matchAsDefault == null ||
                 !matchAsDefault.matcher(value).matches())
                value != FIXED_NONE && value != FIXED_UNDEF
        }
    }

}

class TokenSwitchFieldHandler extends BaseMarcFieldHandler {

    MarcFixedFieldHandler baseConverter
    Map<String, MarcFixedFieldHandler> handlerMap = [:]
    boolean useRecTypeBibLevel = false
    String repeatedAddLink = null
    Map tokenNames = [:]

    TokenSwitchFieldHandler(ruleSet, tag, Map fieldDfn, tokenMapKey='tokenTypeMap') {
        super(ruleSet, tag, fieldDfn)
        assert !link || repeatable // this kind should always be repeatable if linked
        if (fieldDfn['match-repeated']) {
            repeatedAddLink = fieldDfn['match-repeated'].addLink
        }
        this.baseConverter = new MarcFixedFieldHandler(ruleSet, tag, fieldDfn)
        def tokenMap = fieldDfn[tokenMapKey]
        if (tokenMapKey == 'recTypeBibLevelMap') {
            this.useRecTypeBibLevel = true
            buildHandlersByRecTypeBibLevel(fieldDfn, tokenMap)
        } else {
            buildHandlersByTokens(fieldDfn, tokenMap)
        }
        int maxFieldSize = 0
        handlerMap.values().each {
            if (it.fieldSize > maxFieldSize) {
                maxFieldSize = it.fieldSize
            }
        }
        handlerMap.values().each {
            it.fieldSize = maxFieldSize
        }
    }

    private void buildHandlersByRecTypeBibLevel(fieldDfn, recTypeBibLevelMap) {
        recTypeBibLevelMap.each { recTypes, nameBibLevelMap ->
            nameBibLevelMap.each { typeName, bibLevels ->
                recTypes.split(/,/).each { recType ->
                    bibLevels.each {
                        def token = recType + it
                        addHandler(token, fieldDfn[typeName])
                        tokenNames[token] = typeName
                    }
                }
            }
        }
    }

    private void buildHandlersByTokens(fieldDfn, tokenMap) {
        if (tokenMap instanceof String) {
            tokenMap = fieldDfn[tokenMap].tokenMap
        }
        tokenMap.each { token, typeName ->
            addHandler(token, fieldDfn[typeName])
            tokenNames[token] = typeName
        }
    }

    private void addHandler(token, dfn) {
        handlerMap[token] = new MarcFixedFieldHandler(ruleSet, tag, dfn)
    }

    String getToken(leader, value) {
        if (useRecTypeBibLevel) {
            def typeOfRecord = ruleSet.conversion.getTypeOfRecord(leader)
            def bibLevel = ruleSet.conversion.getBibLevel(leader)
            return typeOfRecord + bibLevel
        } else if (value) {
            return value[0]
        } else {
            return value
        }
    }

    ConvertResult convert(state, value) {
        def token = getToken(state.sourceMap.leader, value)
        def converter = handlerMap[token]
        if (converter == null)
            return FAIL

        def addLink = this.link
        if (state.sourceMap[tag].size() > 1 && repeatedAddLink) {
            addLink = repeatedAddLink
        }
        def entityMap = state.entityMap
        if (addLink) {
            def ent = entityMap['?thing']
            def newEnt = newEntity(state, null)
            addValue(ent, addLink, newEnt, true)
            state = state.clone()
            state.entityMap = entityMap.clone()
            state.entityMap['?thing'] = newEnt
        }

        def baseOk = true
        if (baseConverter)
            baseOk = baseConverter.convert(state, value)
        def ok = converter.convert(state, value).ok
        return new ConvertResult(baseOk && ok)
    }

    def revert(Map data, Map result) {
        def entities = [data]
        if (link) {
            entities = getEntity(data).get(link) ?: []
        }
        def values = []
        for (entity in entities) {
            def value = null
            if (baseConverter)
                value = baseConverter.revert(entity, result, true)
            def tokenBasedConverter = handlerMap[getToken(result.leader, value)]
            if (tokenBasedConverter) {
                def overlay = tokenBasedConverter.revert(entity, result, true)
                if (value.size() == 1) {
                    value = value + overlay.substring(1)
                } else {
                    def combined = value.split('')
                    overlay.eachWithIndex { c, i ->
                        if (c != " ") {
                            combined[i] = c
                        }
                    }
                    value = combined.join("")
                }
            }
            if (value.find { it != MarcFixedFieldHandler.FIXED_NONE &&
                    it != MarcFixedFieldHandler.FIXED_UNDEF }) {
                values << value
            }
        }
        return values
    }

}

class MarcSimpleFieldHandler extends BaseMarcFieldHandler {

    static final DateTimeFormatter DT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.n]XXX")
    static final DateTimeFormatter DT_FORMAT_FALLBACK =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.n]XX")

    static final String URI_SLOT = '{_}'

    String property
    String uriTemplate
    Pattern matchUriToken = null
    boolean parseZeroPaddedNumber
    DateTimeFormatter dateTimeFormat
    ZoneId timeZone
    LocalTime defaultTime
    boolean missingCentury = false
    boolean ignored = false
    // TODO: working, but not so useful until capable of merging entities..
    //MarcSimpleFieldHandler linkedHandler

    MarcSimpleFieldHandler(ruleSet, tag, fieldDfn) {
        super(ruleSet, tag, fieldDfn)
        super.setTokenMap(this, fieldDfn)
        if (fieldDfn.addProperty) {
            property = fieldDfn.addProperty
            repeatable = true
        } else {
            property = fieldDfn.property
        }
        def parseDateTime = fieldDfn.parseDateTime
        if (parseDateTime) {
            missingCentury = (parseDateTime == "yyMMdd")
            if (missingCentury) {
                parseDateTime = "yy" + parseDateTime
            }
            dateTimeFormat = DateTimeFormatter.ofPattern(parseDateTime)
            if (fieldDfn.timeZone) {
                timeZone = ZoneId.of(fieldDfn.timeZone)
            }
            if (parseDateTime.indexOf("HH") == -1) {
                defaultTime = LocalTime.MIN
            }
        }
        parseZeroPaddedNumber = (fieldDfn.parseZeroPaddedNumber == true)
        ignored = fieldDfn.get('ignored', false)
        uriTemplate = fieldDfn.uriTemplate
        if (fieldDfn.matchUriToken) {
            matchUriToken = Pattern.compile(fieldDfn.matchUriToken)
            if (fieldDfn.spec) {
                fieldDfn.spec.matches.each {
                    assert matchUriToken.matcher(it).matches()
                }
                fieldDfn.spec.notMatches.each {
                    assert !matchUriToken.matcher(it).matches()
                }
            }
        }
        //if (fieldDfn.linkedEntity) {
        //    linkedHandler = new MarcSimpleFieldHandler(ruleSet,
        //            tag + ":linked", fieldDfn.linkedEntity)
        //}
    }

    ConvertResult convert(state, value) {
        if (ignored || !(property || link))
            return

        if (tokenMap) {
            def mapped = tokenMap[value] ?: tokenMap[value.toLowerCase()]
            if (mapped == null) {
                return new ConvertResult(tokenMap.containsKey(value))
            } else if (mapped == false) {
                return OK
            } else {
                value = mapped
            }
        }

        if (dateTimeFormat) {
            def givenValue = value
            try {
                def dateTime = null
                if (defaultTime) {
                    if (missingCentury) {
                        value = (value[0..2] > "70"? "19" : "20") + value
                    }
                    dateTime = ZonedDateTime.of(LocalDate.parse(
                                value, dateTimeFormat),
                            defaultTime, timeZone)
                } else if (timeZone) {
                    dateTime = LocalDateTime.parse(value, dateTimeFormat).atZone(timeZone)
                } else {
                    dateTime = ZonedDateTime.parse(value, dateTimeFormat)
                }
                value = dateTime.format(DT_FORMAT)
            } catch (DateTimeParseException e) {
                value = givenValue
            }
        } else if (parseZeroPaddedNumber && value) {
            if (value) {
                try {
                    value = Integer.parseInt(value.trim())
                } catch (NumberFormatException e) {
                    ; // pass
                }
            }
        }

        def ent = state.entityMap[aboutEntityName]
        if (definesDomainEntityType) {
            ent['@type'] = definesDomainEntityType
        }


        if (ent == null)
            return FAIL
        if (link) {
            def newEnt = newEntity(state, resourceType)
            addValue(ent, link, newEnt, repeatable)
            ent = newEnt
        }
        if (uriTemplate) {
            if (!matchUriToken || matchUriToken.matcher(value).matches()) {
                ent['@id'] = uriTemplate.replace(URI_SLOT, value.trim())
            } else {
                ent['@value'] = value
            }
        //} else if (linkedHandler) {
        //    linkedHandler.convert(state, value,["?thing": ent])
        } else {
            addValue(ent, property, value, repeatable)
        }

        return OK
    }

    def revert(Map data, Map result) {
        def entity = getEntity(data)
        if (link)
            entity = entity[link]
        if (property) {
            def v = entity[property]
            if (v) {
                if (dateTimeFormat) {
                    try {
                        def zonedDateTime = parseDate(v)
                        def value = zonedDateTime.format(dateTimeFormat)
                        if (missingCentury) {
                            value = value.substring(2)
                        }
                        return value
                    } catch (DateTimeParseException e) {
                        return v
                    }
                } else if (parseZeroPaddedNumber) {
                    try {
                        return String.format("%03d", v)
                    } catch (UnknownFormatConversionException) {
                        return null
                    }
                }
            }
            return revertObject(v)
        } else {
            def entities = entity instanceof List? entity : [entity]
            return entities.collect {
                def id = it instanceof Map? it['@id'] : it
                if (uriTemplate) {
                    def token = extractToken(uriTemplate, id)
                    if (token) {
                        return revertObject(token)
                    }
                }
            }
            return null
        }
    }

    static ZonedDateTime parseDate(String s) {
        try {
            return ZonedDateTime.parse(s, DT_FORMAT)
        } catch (DateTimeParseException e) {
            return ZonedDateTime.parse(s, DT_FORMAT_FALLBACK)
        }
    }

    static String extractToken(tplt, value) {
        if (!value)
            return null
        def i = tplt.indexOf(URI_SLOT)
        if (i > -1) {
            def before = tplt.substring(0, i)
            def after = tplt.substring(i + URI_SLOT.size())
            if (value.startsWith(before) && value.endsWith(after)) {
                def part = value.substring(before.size())
                return part.substring(0, part.size() - after.size())
            }
        } else {
            return null
        }
    }

}

@Log
class MarcFieldHandler extends BaseMarcFieldHandler {

    MarcSubFieldHandler ind1
    MarcSubFieldHandler ind2
    List<String> dependsOn
    UriTemplate uriTemplate
    Map uriTemplateDefaults
    Map computeLinks
    List splitLinkRules
    Map<String, MarcSubFieldHandler> subfields = [:]
    List matchRules = []
    Map pendingResources

    static GENERIC_REL_URI_TEMPLATE = UriTemplate.fromTemplate("generic:{_}")

    MarcFieldHandler(ruleSet, tag, fieldDfn) {
        super(ruleSet, tag, fieldDfn)
        ind1 = fieldDfn.i1? new MarcSubFieldHandler(this, "ind1", fieldDfn.i1) : null
        ind2 = fieldDfn.i2? new MarcSubFieldHandler(this, "ind2", fieldDfn.i2) : null
        pendingResources = fieldDfn.pendingResources

        dependsOn = fieldDfn.dependsOn

        if (fieldDfn.uriTemplate) {
            uriTemplate = UriTemplate.fromTemplate(fieldDfn.uriTemplate)
            uriTemplateDefaults = fieldDfn.uriTemplateDefaults
        }

        computeLinks = (fieldDfn.computeLinks)? new HashMap(fieldDfn.computeLinks) : [:]
        if (computeLinks) {
            computeLinks.use = computeLinks.use.replaceFirst(/^\$/, '')
        }
        splitLinkRules = fieldDfn.splitLink.collect {
            [codes: new HashSet(it.codes),
                link: it.link ?: it.addLink,
                spliceEntityName: it.spliceEntity,
                repeatable: 'addLink' in it]
        }

        def matchDomain = fieldDfn['match-domain']
        if (matchDomain) {
            matchRules << new DomainMatchRule(this, fieldDfn, 'match-domain', matchDomain)
        }
        def matchI1 = fieldDfn['match-i1']
        if (matchI1) {
            matchRules << new IndMatchRule(this, fieldDfn, 'match-i1', matchI1, 'ind1')
        }
        def matchI2 = fieldDfn['match-i2']
        if (matchI2) {
            matchRules << new IndMatchRule(this, fieldDfn, 'match-i2', matchI2, 'ind2')
        }
        def matchCode = fieldDfn['match-code']
        if (matchCode) {
            matchRules << new CodeMatchRule(this, fieldDfn, 'match-code', matchCode)
        }
        def matchPattern = fieldDfn['match-pattern']
        if (matchPattern) {
            matchRules << new CodePatternMatchRule(
            this, fieldDfn, 'match-pattern', matchPattern)
        }

        fieldDfn.each { key, obj ->
            def m = key =~ /^\$(\w+)$/
            if (m) {
                addSubfield(m[0][1], obj)
            }
        }
        if (splitLinkRules) {
            assert resourceType, "splitLinks requires resourceType in: ${fieldDfn}"
        }
    }

    void addSubfield(code, dfn) {
        subfields[code] = dfn? new MarcSubFieldHandler(this, code, dfn) : null
    }

    ConvertResult convert(state, value) {

        if (!(value instanceof Map)) {
            throw new MalformedFieldValueException()
        }

        def aboutEntity = state.entityMap[aboutEntityName]
        if (aboutEntity == null)
            return FAIL

        if (definesDomainEntityType) {
            aboutEntity['@type'] = definesDomainEntityType
        }

        for (rule in matchRules) {
            def handler = rule.getHandler(aboutEntity, value)
            if (handler) {
                // TODO: resolve combined config
                return handler.convert(state, value)
            }
        }

        def entity = aboutEntity

        def handled = new HashSet()

        def linkage = computeLinkage(state, entity, value, handled)
        if (linkage.newEntity) {
            entity = linkage.newEntity
        }

        def uriTemplateParams = [:]

        def unhandled = new HashSet()

        def localEntites = [:]

        [ind1: ind1, ind2: ind2].each { indKey, handler ->
            if (!handler)
                return
            def ok = handler.convertSubValue(state,
                    value[indKey], entity, uriTemplateParams, localEntites)
            if (!ok && handler.marcDefault == null) {
                unhandled << indKey
            }
        }

        value.subfields.each {
            it.each { code, subVal ->
                def subDfn = subfields[code]
                def ok = false
                if (subDfn) {
                    def ent = (subDfn.aboutEntityName)?
                        state.entityMap[subDfn.aboutEntityName] :
                        (linkage.codeLinkSplits[code] ?: entity)
                    if ((subDfn.requiresI1 && subDfn.requiresI1 != value.ind1) ||
                        (subDfn.requiresI2 && subDfn.requiresI2 != value.ind2)) {
                        ok = true // rule does not apply here
                    } else {
                        ok = subDfn.convertSubValue(state, subVal, ent, uriTemplateParams, localEntites)
                    }
                }
                if (!ok && !handled.contains(code)) {
                    unhandled << code
                }
            }
        }

        if (uriTemplate) {
            uriTemplateDefaults.each { k, v ->
                if (!uriTemplateParams.containsKey(k)) {
                    v = getByPath(aboutEntity, k) ?: v
                    uriTemplateParams[k] = v
                }
            }

            // TODO: need to run before linking resource above to work properly
            // for multiply linked entities. Or, perhaps better, run a final "mapIds" pass
            // in the main loop..
            def computedUri = uriTemplate.expand(uriTemplateParams)
            def altUriRel = "sameAs"
            if (definesDomainEntityType != null) {
                addValue(entity, altUriRel, ['@id': computedUri], true)
            } else {
                if (entity['@id']) {
                    // TODO: ok as precaution?
                    addValue(entity, altUriRel, ['@id': entity['@id']], true)
                }
                entity['@id'] = computedUri
            }
        }

        linkage.splitResults.each {
            if (it.entity.find { k, v -> k != "@type" }) {
                addValue(aboutEntity, it.rule.link, it.entity, it.rule.repeatable)
            }
        }

        return new ConvertResult(unhandled)
    }

    def computeLinkage(state, entity, value, handled) {
        def codeLinkSplits = [:]
        // TODO: clear unused codeLinkSplits afterwards..
        def splitResults = []
        def spliceEntity = null
        if (splitLinkRules) {
            splitLinkRules.each { rule ->
                def newEnt = null
                if (rule.spliceEntityName) {
                    newEnt = ["@type": rule.spliceEntityName]
                    spliceEntity = newEnt
                } else {
                    newEnt = ["@type": resourceType]
                }
                splitResults << [rule: rule, entity: newEnt]
                rule.codes.each {
                    codeLinkSplits[it] = newEnt
                }
            }
        }
        def newEnt = null
        if ((!splitResults || spliceEntity) && resourceType) {
            def useLinks = Collections.emptyList()
            if (computeLinks) {
                def use = computeLinks.use
                def resourceMap = (computeLinks.mapping instanceof Map)?
                        computeLinks.mapping : tokenMaps[computeLinks.mapping]
                def linkTokens = value.subfields.findAll {
                    use in it.keySet() }.collect { it.iterator().next().value }
                useLinks = linkTokens.collect {
                    def linkDfn = resourceMap[it]
                    if (linkDfn == null) {
                        linkDfn = resourceMap[it.toLowerCase().replaceAll(/[^a-z0-9_-]/, '')]
                    }
                    if (linkDfn instanceof Map)
                        linkDfn.term
                    else if (linkDfn instanceof String)
                        linkDfn
                    else
                        GENERIC_REL_URI_TEMPLATE.expand(["_": it])
                }
                if (useLinks.size() > 0) {
                    handled << use
                } else {
                    def link = resourceMap['*']
                    if (link) {
                        useLinks = [link]
                    }
                }
            }

            newEnt = newEntity(state, resourceType)

            def lRepeatLink = repeatable
            if (useLinks) {
                lRepeatLink = true
            }
            if (spliceEntity) {
                entity = spliceEntity
            }

            // TODO: use @id (existing or added bnode-id) instead of duplicating newEnt
            def entRef = newEnt
            if (useLinks && link) {
                if (!newEnt['@id'])
                    newEnt['@id'] = "_:b-${state.bnodeCounter++}" as String
                entRef = ['@id': newEnt['@id']]
            }
            if (link) {
                addValue(entity, link, newEnt, repeatable)
            }
            useLinks.each {
                addValue(entity, it, entRef, lRepeatLink)
            }
        }
        return [
            codeLinkSplits: codeLinkSplits,
            splitResults: splitResults,
            newEntity: newEnt
        ]
    }

    String getByPath(entity, path) {
        path.split(/\./).each {
            if (entity) {
                entity = entity[it]
                if (entity instanceof List) {
                    entity = entity[0]
                }
            }
        }
        return (entity instanceof String)? entity : null
    }

    Map getLocalEntity(Map state, Map owner, String id, Map localEntites) {
        def entity = localEntites[id]
        if (entity == null) {
            def pending = pendingResources[id]
            entity = localEntites[id] = newEntity(state, pending.resourceType)
            def link = pending.link ?: pending.addLink
            addValue(owner, link, entity, pending.containsKey('addLink'))
        }
        return entity
    }

    def revert(Map data, Map result, MatchCandidate matchCandidate=null) {

        def matchedResults = []

        if (matchCandidate == null) {
            for (rule in matchRules) {
                for (candidate in rule.candidates) {
                    matchedResults += candidate.handler.revert(data, result, candidate)
                }
            }
        }

        def entity = getEntity(data)

        def types = entity['@type']
        if (types instanceof String) { types = [types] }
        if (definesDomainEntityType && !(types.contains(definesDomainEntityType)))
            return null

        def entities = [entity]

        def results = []

        def useLinks = []
        if (computeLinks && computeLinks.mapping instanceof Map) {
            computeLinks.mapping.each { code, compLink ->
                if (compLink in entity) {
                    if (code == '*') {
                        useLinks << [link: compLink, subfield: null]
                    } else {
                        useLinks << [link: compLink, subfield: [(computeLinks.use): code]]
                    }
                }
            }
        } else {
            useLinks << [link: link, subfield: null]
        }

        for (useLink in useLinks) {
            def useEntities = entities
            if (useLink.link) {
                useEntities = entity[useLink.link]
                if (!(useEntities instanceof List)) // should be if repeat == true
                    useEntities = useEntities? [useEntities] : []
                if (resourceType) {
                    useEntities = useEntities.findAll {
                        if (!it) return false
                        def type = it['@type']
                        return (type instanceof List)?
                            resourceType in type : type == resourceType
                    }
                }
            }

            def aboutMap = [:]
            if (pendingResources) {
                pendingResources.each { key, pending ->
                    def link = pending.link ?: pending.addLink
                    def resourceType = pending.resourceType
                    useEntities.each {
                        def about = it[link]
                        // TODO: if multiple, spread according to repeated subfield groups(?)...
                        if (about instanceof List) {
                            about = about[0]
                        }
                        if (about && (!resourceType || about['@type'] == resourceType)) {
                            aboutMap[key] = about
                        }
                    }
                }
            }

            useEntities.each {
                def field = revertOne(data, it, null, aboutMap, matchCandidate)
                if (field) {
                    if (useLink.subfield) {
                        field.subfields << useLink.subfield
                    }
                    results << field
                }
            }
        }

        if (splitLinkRules) {
            // TODO: refine, e.g. handle spliceEntityName..
            def resultItems = []
            for (rule in splitLinkRules) {
                def linked = entity[rule.link]
                if (!linked)
                    continue
                if (!(linked instanceof List)) {
                    linked = [linked]
                }
                linked.each {
                    def item = revertOne(data, it, rule.codes)
                    if (item)
                        resultItems << item
                }
            }
            if (resultItems.size() /*&& !repeatable*/) {
                def merged = resultItems[0]
                if (resultItems.size() > 1) {
                    for (map in resultItems[1..-1]) {
                        merged.subfields += map.subfields
                    }
                }
                results << merged
            } else {
                results += resultItems
            }
        }
        return results + matchedResults
    }

    def revertOne(Map data, Map currentEntity, Set onlyCodes=null, Map aboutMap=null,
            MatchCandidate matchCandidate=null) {

        def i1 = ind1? ind1.revert(data, currentEntity) : (matchCandidate?.ind1 ?: ' ')
        def i2 = ind2? ind2.revert(data, currentEntity) : (matchCandidate?.ind2 ?: ' ')

        def subs = []
        def failedRequired = false

        subfields.collect { code, subhandler ->
            if (!subhandler)
                return
            if (onlyCodes && !onlyCodes.contains(code))
                return
            if (subhandler.requiresI1) {
                if (i1 == null) {
                    i1 = subhandler.requiresI1
                } else if (i1 != subhandler.requiresI1) {
                    return
                }
            }
            if (subhandler.requiresI2) {
                if (i2 == null) {
                    i2 = subhandler.requiresI2
                } else if (i2 != subhandler.requiresI2) {
                    return
                }
            }
            def selectedEntity = subhandler.about? aboutMap[subhandler.about] : currentEntity
            if (!selectedEntity)
                return
            def value = subhandler.revert(data, selectedEntity)
            if (value instanceof List) {
                value.each {
                    if (!matchCandidate || matchCandidate.matchValue(code, it)) {
                        subs << [(code): it]
                    }
                }
            } else if (value != null) {
                if (!matchCandidate || matchCandidate.matchValue(code, value)) {
                    subs << [(code): value]
                }
            } else {
                if (subhandler.required || subhandler.requiresI1 || subhandler.requiresI2) {
                    failedRequired = true
                }
            }
        }

        return !failedRequired && i1 != null && i2 != null && subs.length?
            [ind1: i1, ind2: i2, subfields: subs] :
            null
    }
}

class MarcSubFieldHandler extends ConversionPart {

    MarcFieldHandler fieldHandler
    String code
    char[] interpunctionChars
    char[] surroundingChars
    String link
    String about
    boolean repeatable
    String property
    boolean repeatProperty
    String resourceType
    UriTemplate subUriTemplate
    Pattern splitValuePattern
    List<String> splitValueProperties
    String rejoin
    boolean allowEmpty
    Map defaults
    String definedElsewhereToken
    String marcDefault
    boolean required = false
    String requiresI1
    String requiresI2
    String itemPos

    MarcSubFieldHandler(fieldHandler, code, Map subDfn) {
        this.ruleSet = fieldHandler.ruleSet
        this.fieldHandler = fieldHandler
        this.code = code
        aboutEntityName = subDfn.aboutEntity
        interpunctionChars = subDfn.interpunctionChars?.toCharArray()
        surroundingChars = subDfn.surroundingChars?.toCharArray()
        super.setTokenMap(fieldHandler, subDfn)
        link = subDfn.link
        about = subDfn.about
        required = subDfn.required
        repeatable = false
        if (subDfn.addLink) {
            repeatable = true
            link = subDfn.addLink
        }
        property = subDfn.property
        repeatProperty = false
        if (subDfn.addProperty) {
            property = subDfn.addProperty
            repeatProperty = true
        }
        resourceType = subDfn.resourceType
        if (subDfn.uriTemplate) {
            subUriTemplate = UriTemplate.fromTemplate(subDfn.uriTemplate)
        }
        if (subDfn.splitValuePattern) {
            // TODO: support repeatable?
            splitValuePattern = Pattern.compile(subDfn.splitValuePattern)
            splitValueProperties = subDfn.splitValueProperties
            rejoin = subDfn.rejoin
            allowEmpty = subDfn.allowEmpty
        }
        defaults = subDfn.defaults
        marcDefault = subDfn.marcDefault
        definedElsewhereToken = subDfn.definedElsewhereToken
        requiresI1 = subDfn['requires-i1']
        requiresI2 = subDfn['requires-i2']
        itemPos = subDfn.itemPos
    }

    boolean convertSubValue(state, subVal, ent, uriTemplateParams, localEntites) {
        def ok = false
        def uriTemplateKeyBase = ""

        if (subVal)
            subVal = clearChars(subVal)
        else
            return true // no value, treat as not present

        if (tokenMap) {
            if (subVal == definedElsewhereToken) {
                return true
            }
            subVal = tokenMap[subVal]
            if (subVal == null) {
                return false
            }
        }

        if (about) {
            ent = fieldHandler.getLocalEntity(state, ent, about, localEntites)
        }

        if (link) {
            def entId = null
            if (subUriTemplate) {
                try {
                    entId = subUriTemplate.expand(["_": subVal])
                } catch (IllegalArgumentException e) {
                    // TODO: improve?
                    // bad characters in what should have been a proper URI path ({+_} expansion)
                }
            }
            def newEnt = newEntity(state, resourceType, entId)
            addValue(ent, link, newEnt, repeatable)
            ent = newEnt
            uriTemplateKeyBase = "${link}."
            ok = true
        }

        def didSplit = false
        if (splitValuePattern) {
            def m = splitValuePattern.matcher(subVal)
            if (m) {
                splitValueProperties.eachWithIndex { prop, i ->
                    def v = m[0][i + 1]
                    if (v) {
                        ent[prop] = v
                    }
                    fieldHandler.addValue(uriTemplateParams, uriTemplateKeyBase + prop, v, true)
                }
                didSplit = true
                ok = true
            }
        }
        if (!didSplit && property) {
            fieldHandler.addValue(ent, property, subVal, repeatProperty)
            fieldHandler.addValue(uriTemplateParams, uriTemplateKeyBase + property, subVal, true)
            ok = true
        }

        if (defaults) {
            defaults.each { k, v -> if (v != null && !(k in ent)) ent[k] = v }
        }
        return ok
    }

    String clearChars(subVal) {
        def val = subVal.trim()
        if (val.size() > 2) {
            if (interpunctionChars) {
                for (c in interpunctionChars) {
                    if (val.size() < 2) {
                        break
                    }
                    if (val[-1].equals(c.toString())) {
                        val = val[0..-2].trim()
                    }
                }
            }
            if (surroundingChars) {
                for (c in surroundingChars) {
                    if (val.size() < 2) {
                        break
                    }
                    if (val[-1].equals(c.toString())) {
                        val = val[0..-2].trim()
                    } else if (val[0].equals(c.toString())) {
                        val = val[1..-1].trim()
                    }
                }
        }
            return val.toString()
        }
        return val
    }

    def revert(Map data, Map currentEntity) {
        currentEntity = aboutEntityName? getEntity(data) : currentEntity
        if (currentEntity == null)
            return null

        def entities = link? currentEntity[link] : [currentEntity]
        if (entities == null) {
            return null
        }
        if (entities instanceof Map) {
            entities = [entities]
        }

        def values = []

        boolean rest = false
        for (entity in entities) {
            if (!rest) {
                rest = true
                if (itemPos == "rest")
                    continue
            } else if (itemPos == "first")
                break

            if (resourceType && entity['@type'] != resourceType)
                continue

            // TODO: match defaults only if not set by other subfield...
            if (defaults && defaults.any { p, o -> o == null && (p in entity) || entity[p] != o })
                continue

            if (splitValueProperties && rejoin) {
                def vs = []
                boolean allEmpty = true
                splitValueProperties.each {
                    def v = entity[it]
                    if (v == null && allowEmpty) {
                        v = ""
                    } else {
                        allEmpty = false
                    }
                    if (v != null) {
                        vs << v
                    }
                }
                if (vs.size() == splitValueProperties.size() && !allEmpty) {
                    values << vs.join(rejoin)
                    continue
                }
            }

            def value = null
            if (property) {
                value = revertObject(entity[property])
            } else if (link) {
                def obj = entity['@id']
                if (obj && subUriTemplate) {
                    def tpltStr = subUriTemplate.template
                    // NOTE: requires variable slot to be at end of template
                    // TODO: unify with extractToken
                    def exprIdx = tpltStr.indexOf('{_}')
                    if (exprIdx > -1) {
                        assert tpltStr.size() == exprIdx + 3
                        obj = URLDecoder.decode(obj.substring(exprIdx))
                    } else {
                        exprIdx = tpltStr.indexOf('{+_}')
                        if (exprIdx > -1) {
                            assert tpltStr.size() == exprIdx + 4
                            obj = obj.substring(exprIdx)
                        }
                    }
                }
                value = revertObject(obj)
            }
            if (value != null) {
                values << value
            } else if (marcDefault) {
                values << marcDefault
            }
        }

        if (values.size() == 0)
            return null
        else if (values.size() == 1)
            return values[0]
        else
            return values
    }

}

abstract class MatchRule {

    static matchRuleKeys = [
        'match-domain', 'match-i1', 'match-i2', 'match-code', 'match-pattern'
    ]

    Map ruleMap = [:]

    MatchRule(handler, fieldDfn, ruleKey, rules) {
        rules.each { key, matchDfn ->
            def comboDfn = fieldDfn.clone()
            // create nested combinations, but prevent recursive nesting
            if (comboDfn.nestedMatch) {
                matchRuleKeys.each {
                    comboDfn.remove(it)
                }
            } else {
                def newRule = comboDfn[ruleKey].clone()
                newRule.remove(key)
                comboDfn[ruleKey] = newRule
                comboDfn.nestedMatch = true
            }
            comboDfn += matchDfn
            def tag = null
            ruleMap[key] = new MarcFieldHandler(handler.ruleSet, tag, comboDfn)
        }
    }

    MarcFieldHandler getHandler(entity, value) {
        for (String key : getKeys(entity, value)) {
            def handler = ruleMap[key]
            if (handler) {
                return handler
            }
        }
        return null
    }

    List<String> getKeys(entity, value) {
        return [getSingleKey(entity, value)]
    }

    abstract String getSingleKey(entity, value)

    List<MatchCandidate> getCandidates() {
        return []
    }

}

class MatchCandidate {
    String ind1
    String ind2
    MarcFieldHandler handler
    String code
    Pattern pattern
    boolean matchValue(String code, String value) {
        if (!pattern) {
            return true
        } else {
            return (code == this.code) && pattern.matcher(value)
        }
    }
}

class DomainMatchRule extends MatchRule {
    DomainMatchRule(handler, fieldDfn, ruleKey, rules) {
        super(handler, fieldDfn, ruleKey, rules)
    }
    List<String> getKeys(entity, value) {
        def types = entity['@type']
        return types instanceof String? [types] : types
    }
    String getSingleKey(entity, value) {
        throw new UnsupportedOperationException()
    }
}

class IndMatchRule extends MatchRule {
    String indKey
    IndMatchRule(handler, fieldDfn, ruleKey, rules, indKey) {
        super(handler, fieldDfn, ruleKey, rules)
        this.indKey = indKey
    }
    String getSingleKey(entity, value) {
        return value[indKey]
    }
    List<MatchCandidate> getCandidates() {
        return ruleMap.collect { token, handler ->
            new MatchCandidate(handler: handler, (indKey): token)
        }
    }
}

class CodeMatchRule extends MatchRule {
    CodeMatchRule(handler, fieldDfn, ruleKey, rules) {
        super(handler, fieldDfn, ruleKey, parseRules(rules))
    }
    static Map parseRules(Map rules) {
        def parsed = [:]
        rules.each { key, map ->
            key.split().each { parsed[it] = map }
        }
        return parsed
    }
    String getSingleKey(entity, value) {
        for (sub in value.subfields) {
            for (key in sub.keySet()) {
                if (key in ruleMap)
                    return key
            }
        }
    }
    List<MatchCandidate> getCandidates() {
        def candidates = []
        for (handler in ruleMap.values()) {
            candidates << new MatchCandidate(handler: handler)
            break
        }
        return candidates
    }
}

class CodePatternMatchRule extends MatchRule {
    Map<String, Map> patterns = [:]
    CodePatternMatchRule(handler, fieldDfn, ruleKey, rules) {
        super(handler, fieldDfn, ruleKey, parseRules(rules))
        fillPatternCombos(rules)
    }
    static Map parseRules(Map rules) {
        def parsed = [:]
        rules.each { code, patternMap ->
            assert code[0] == '$' // IMPROVE: don't require code prefix?
            code = code.substring(1)
            patternMap.each { pattern, map ->
                parsed["${code} ${pattern}"] = map
            }
        }
        return parsed
    }
    Map fillPatternCombos(Map rules) {
        rules.each { code, patternMap ->
            code = code.substring(1)
            patternMap.each { pattern, map ->
                patterns[code] = [pattern: ~pattern, key: "${code} ${pattern}"]
            }
        }
    }
    String getSingleKey(entity, value) {
        def key
        for (sub in value.subfields) {
            sub.each { code, codeval ->
                def combo = patterns[code]
                if (combo && combo.pattern.matcher(codeval))
                    key = combo.key
            }
        }
        return key
    }
    List<MatchCandidate> getCandidates() {
        return patterns.collect { code, patternDfn ->
            new MatchCandidate(
                    handler: ruleMap[patternDfn.key],
                    code: code,
                    pattern: patternDfn.pattern)
        }
    }
}

class MalformedFieldValueException extends RuntimeException {}

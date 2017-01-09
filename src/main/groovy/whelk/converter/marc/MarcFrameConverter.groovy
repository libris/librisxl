package whelk.converter.marc

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.JsonLd
import whelk.converter.FormatConverter

import java.time.*
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.Temporal
import java.util.regex.Pattern

import static com.damnhandy.uri.template.UriTemplate.fromTemplate
import static groovy.transform.TypeCheckingMode.SKIP

@Log
class MarcFrameConverter implements FormatConverter {

    ObjectMapper mapper = new ObjectMapper()
    String cfgBase = "ext"

    protected MarcConversion conversion

    MarcFrameConverter() {
        def config = readConfig("$cfgBase/marcframe.json") {
            mapper.readValue(it, Map)
        }
        initialize(config)
    }

    MarcFrameConverter(Map config) {
        initialize(config)
    }

    def readConfig(String path, Closure picker) {
        return getClass().classLoader.getResourceAsStream(path).withStream(picker)
    }

    void initialize(Map config) {
        def tokenMaps = loadTokenMaps(config.tokenMaps)
        conversion = new MarcConversion(config, tokenMaps)
    }

    Map loadTokenMaps(tokenMaps) {
        def result = [:]
        def maps = [:]
        if (tokenMaps instanceof String) {
            tokenMaps = [tokenMaps]
        }
        if (tokenMaps instanceof List) {
            tokenMaps.each {
                if (it instanceof String) {
                    maps += readConfig("$cfgBase/$it") {
                        mapper.readValue(it, Map)
                    }
                } else {
                    maps += it
                }
            }
        } else {
            maps = tokenMaps
        }
        maps.each { key, src ->
            if (src instanceof String) {
                result[key] = readConfig("$cfgBase/$src") {
                    mapper.readValue(it, Map)
                }
            } else {
                result[key] = src
            }
        }
        return result
    }

    Map runConvert(Map marcSource, String recordId = null, Map extraData = null) {
        return conversion.convert(marcSource, recordId, extraData)
    }

    Map runRevert(Map data) {
        if (data['@graph']) {
            def entryId = data['@graph'][0]['@id']
            data = JsonLd.frame(entryId, data)
        }
        return conversion.revert(data)
    }

    @Override
    String getResultContentType() { "application/ld+json" }

    @Override
    String getRequiredContentType() { "application/x-marc-json" }

    @Override
    Map convert(Map source, String id) {
        def result = runConvert(source, id, null)
        log.trace("Created frame: $result")
        return result
    }

    Map convert(Map source, String id, Map extraData) {
        def result = runConvert(source, id, extraData)
        log.trace("Created frame: $result")
        return result
    }

    public static void main(String[] args) {
        List fpaths
        def cmd = "convert"
        if (args.length > 1) {
            cmd = args[0]
            fpaths = args[1..-1]
        } else {
            fpaths = args[0..-1]
        }
        def converter = new MarcFrameConverter()

        for (fpath in fpaths) {
            def source = converter.mapper.readValue(new File(fpath), Map)
            def result = null
            if (cmd == "revert") {
                result = converter.runRevert(source)
            } else {
                result = converter.runConvert(source, fpath)
            }
            if (fpaths.size() > 1)
                println "SOURCE: ${fpath}"
            println converter.mapper.writeValueAsString(result)
        }
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

    URI baseUri = Document.BASE_URI
    String someUriTemplate
    Set someUriVars

    MarcConversion(Map config, Map tokenMaps) {
        marcTypeMap = config.marcTypeFromTypeOfRecord
        this.tokenMaps = tokenMaps
        //this.baseUri = new URI(config.baseUri ?: '/')
        if (config.someUriTemplate) {
            someUriTemplate = config.someUriTemplate
            someUriVars = fromTemplate(someUriTemplate).getVariables() as Set
        }

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

    String resolve(String uri) {
        if (uri == null)
            return null
        return baseUri.resolve(uri).toString()
    }

    Map convert(Map marcSource, String recordId = null, Map extraData = null) {

        def leader = marcSource.leader
        def marcCat = getMarcCategory(leader)
        def marcRuleSet = marcRuleSets[marcCat]

        def marcRemains = [failedFixedFields: [:], uncompleted: [], broken: []]

        def state = [
                sourceMap     : [leader: leader],
                bnodeCounter  : 0,
                entityMap     : [:],
                quotedEntities: [],
                marcRemains   : marcRemains
        ]

        marcRuleSet.convert(marcSource, state)

        marcRuleSet.processExtraData(state.entityMap, extraData)

        def record = state.entityMap['?record']
        def thing = state.entityMap['?thing']

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

        record["@id"] = resolve(recordId)

        marcRuleSet.completeEntities(state.entityMap)

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
            return toFlatQuotedForm(state, marcRuleSet)
        } else {
            return record
        }
    }

    def toFlatQuotedForm(state, marcRuleSet) {
        marcRuleSet.topPendingResources.each { key, dfn ->
            if (dfn.about && dfn.link) {
                def ent = state.entityMap[dfn.about]
                def linked = ent[dfn.link]
                if (linked) {
                    def linkedId = linked['@id']
                    if (linkedId) {
                        ent[dfn.link] = ['@id': linkedId]
                    }
                }
            }
        }

        def quotedEntities = []
        def quotedIds = new HashSet()

        state.quotedEntities.each { ent ->
            // TODO: move to quoted-processing step in storage loading?
            def entId = ent['@id']
            ?: ent['sameAs']?.getAt(0)?.get('@id')
            ?: makeSomeId(ent)
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

        def entities = state.entityMap.values()
        return [
                '@graph': entities + quotedEntities
        ]
    }

    String someUriValuesVar = 'q'
    String someUriDataVar = 'data'

    String makeSomeId(Map ent) {
        def data = [:]
        collectUriData(ent, data)
        if (someUriValuesVar in someUriVars) {
            data[someUriValuesVar] = data.findResults { k, v ->
                k in someUriVars ? null : v
            }
        }
        if (someUriDataVar in someUriVars) {
            data[someUriDataVar] = data.clone()
        }
        return resolve(fromTemplate(someUriTemplate).expand(data))
    }

    void collectUriData(Map obj, Map acc, String path = '') {
        obj.each { k, v ->
            def key = k[0] == '@' ? k.substring(1) : k
            def vs = v instanceof List ? v : [v]
            vs.each {
                def keypath = path + key
                if (it instanceof Map) {
                    collectUriData(it, acc, keypath + '.')
                } else {
                    acc[keypath] = it
                }
            }
        }
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

    // TODO: too rigid, change to topPendingResourcesChain computed from topPendingResources
    String thingLink
    String definingTrait

    def fieldHandlers = [:]
    List<MarcFramePostProcStep> postProcSteps

    Set primaryTags = new HashSet()
    Map<String, Set<String>> aboutTypeMap = new HashMap<String, Set<String>>()

    Map topPendingResources = [:]

    Map oaipmhSetSpecPrefixMap = [:]

    MarcRuleSet(conversion, name) {
        this.conversion = conversion
        this.name = name
        this.aboutTypeMap['?record'] = new HashSet<String>()
        this.aboutTypeMap['?thing'] = new HashSet<String>()
    }

    void buildHandlers(Map config) {
        def subConf = config[name]

        if (config.pendingResources) {
            topPendingResources.putAll(config.pendingResources)
        }

        subConf.each { tag, dfn ->

            if (tag == 'postProcessing') {
                postProcSteps = dfn.collect {
                    conversion.parsePostProcStep(it)
                }.findAll()
                return
            } else if (tag == 'pendingResources') {
                topPendingResources.putAll(dfn)
                return
            } else if (tag == 'oaipmhSetSpecPrefixMap') {
                oaipmhSetSpecPrefixMap = dfn
                return
            }

            thingLink = topPendingResources['?thing'].link
            definingTrait = topPendingResources['?work']?.link

            dfn = processInherit(config, subConf, tag, dfn)

            dfn = processInclude(config, dfn, tag)

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
            } else {
                handler = new MarcSimpleFieldHandler(this, tag, dfn)
                assert handler.property || handler.uriTemplate, "Incomplete: $tag: $dfn"
            }
            fieldHandlers[tag] = handler
            if (dfn.aboutType && dfn.aboutType != 'Record') {
                aboutTypeMap[dfn.aboutEntity ?: '?thing'] << dfn.aboutType
            }
        }
    }

    def processInherit(config, subConf, tag, fieldDfn) {
        def ref = fieldDfn.inherit
        if (!ref) {
            return fieldDfn
        }

        def refTag = tag
        if (ref.contains(':')) {
            (ref, refTag) = ref.split(':')
        }
        def baseDfn = (ref in subConf) ? subConf[ref] : config[ref][refTag]
        if (baseDfn.inherit) {
            subConf = (ref in config) ? config[ref] : subConf
            baseDfn = processInherit(config, subConf, ref ?: refTag, baseDfn)
        }
        def merged = baseDfn + fieldDfn
        merged.remove('inherit')
        return merged
    }

    static Map processInclude(Map config, Map fieldDfn, String tag = "N/A") {
        def includes = fieldDfn.include
        if (!includes) {
            return fieldDfn
        }
        def merged = [:] + fieldDfn
        for (include in Util.asList(includes)) {
            def baseDfn = config['patterns'][include]
            assert tag && baseDfn
            if (baseDfn.include) {
                baseDfn = processInclude(config, baseDfn, tag)
            }
            assert tag && !(baseDfn.keySet().intersect(fieldDfn.keySet()) - 'include')
            merged += baseDfn
            merged.remove('include')
        }
        return merged
    }

    boolean matchesData(Map data) {
        // TODO:
        // - Should be one of instanceOf, itemOf, focus? Must not collide.
        // - Remove type-matching altogether?
        if (definingTrait && data[thingLink] && data[thingLink][definingTrait])
            return true

        if (hasIntersection(Util.asList(data['@type']), aboutTypeMap['?record']))
            return true
        def thing = data[thingLink]
        if (!thing)
            return false
        return hasIntersection(Util.asList(thing['@type']), aboutTypeMap['?thing'])
    }

    boolean hasIntersection(Collection candidates, Collection matches) {
        for (value in candidates) {
            if (value in matches)
                return true
        }
    }

    void convert(Map marcSource, Map state) {
        def preprocFields = []
        def primaryFields = []
        def otherFields = []

        topPendingResources.each { key, dfn ->
            state.entityMap[key] = [:]
        }

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
        for (field in fields) {
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

    void processExtraData(Map entityMap, Map extraData) {
        extraData?.get("oaipmhSetSpecs")?.each {
            def cIdx = it.indexOf(':')
            if (cIdx == -1)
                return
            def prefix = it.substring(0, cIdx)
            def value = it.substring(cIdx + 1)

            def dfn = oaipmhSetSpecPrefixMap[prefix]
            if (!dfn)
                return

            def about = entityMap[dfn.about]
            if (about.containsKey(dfn.link))
                return

            def target = [:]
            about[dfn.link] = target
            if (dfn.resourceType)
                target["@type"] = dfn.resourceType
            if (dfn.property) {
                target[dfn.property] = value
            }
            if (dfn.uriTemplate) {
                def uri = conversion.resolve(fromTemplate(
                        dfn.uriTemplate).set('_', value).set(target).expand())
                target["@id"] = uri
            }
        }
    }

    void completeEntities(Map entityMap) {
        def record = entityMap['?record']
        def givenRecId = record['@id']

        topPendingResources.each { key, dfn ->
            def entity = entityMap[key]
            if (!entity && !dfn.addEmpty)
                return

            if (!entity['@type']) {
                entity['@type'] = dfn.resourceType
            }

            def entityId = entity['@id']
            def builtEntityId = null

            if (dfn.uriTemplate) {
                try {
                    builtEntityId = conversion.resolve(
                            fromTemplate(dfn.uriTemplate)
                                    .set('marcType', name).set(record).expandPartial())
                } catch (IllegalArgumentException e) {
                    ; // Fails on resolve if expanded is only partially filled
                }
            }

            if (!entityId && givenRecId && dfn.fragmentId) {
                entityId = entity['@id'] = givenRecId + '#' + dfn.fragmentId
            }

            if (builtEntityId) {
                if (!entityId) {
                    entity['@id'] = builtEntityId
                } else {
                    entity.get('sameAs', []) << ['@id': builtEntityId]
                }
            }

            if (dfn.about && dfn.link) {
                def about = entityMap[dfn.about]
                def existing = about[dfn.link]
                if (existing) {
                    entity.each { k, v ->
                        if (!existing.containsKey(k)) {
                            existing[k] = v
                        }
                    }
                    entityMap[key] = existing
                } else {
                    about[dfn.link] = entity
                }
            }
        }
    }

}


@CompileStatic
class ConversionPart {

    MarcRuleSet ruleSet
    String aboutEntityName
    Map tokenMap
    Map reverseTokenMap
    boolean embedded = false

    void setTokenMap(BaseMarcFieldHandler fieldHandler, Map dfn) {
        def tokenMap = dfn.tokenMap
        if (tokenMap) {
            reverseTokenMap = [:]
            this.tokenMap = (Map) ((tokenMap instanceof String) ?
                    fieldHandler.tokenMaps[tokenMap] : tokenMap)
            this.tokenMap.each { k, v ->
                if (v != null) {
                    reverseTokenMap[v] = k
                }
            }
        }
    }

    Map getEntity(Map data) {
        /* TODO: build topPendingResources once then revert all fields
        for (linkStep in topPendingResourcesChain[aboutEntityName]) {
            def child = data[linkStep]
            if (child) {
                data = child
            } else {
                return null
            }
        }
        return data
        */
        if (aboutEntityName == '?record') {
            return data
        }
        if (ruleSet.thingLink in data) {
            data = (Map) data[ruleSet.thingLink]
            if (aboutEntityName != '?thing' && ruleSet.definingTrait in data) {
                data = (Map) data[ruleSet.definingTrait]
            }
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

    Map newEntity(Map state, String type, String id = null, Boolean embedded = embedded) {
        def ent = [:]
        if (type) ent["@type"] = type
        if (id) ent["@id"] = id
        if (!embedded) {
            ((List) state.quotedEntities) << ent
        }
        return ent
    }

    static void addValue(Map obj, String key, value, boolean repeatable) {
        def current = obj[key]
        if (current && !(current instanceof List)) {
            // TODO: if this happens, data is odd or repeatable should be true
            current = [current]
        }
        if (current || repeatable) {
            List l = (List) current ?: []

            String vId = value instanceof Map ? value["@id"] : null
            if (vId) {
                def existing = l.find { it instanceof Map && it["@id"] == vId }
                if (existing) {
                    ((Map) value).putAll((Map) existing)
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

@CompileStatic
abstract class BaseMarcFieldHandler extends ConversionPart {

    String tag
    Map tokenMaps
    String definesDomainEntityType
    String link
    boolean repeatable = false
    String resourceType
    Map linkRepeated = null

    static final ConvertResult OK = new ConvertResult(true)
    static final ConvertResult FAIL = new ConvertResult(false)

    BaseMarcFieldHandler(MarcRuleSet ruleSet, String tag, Map fieldDfn) {
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
        resourceType = fieldDfn.resourceType
        embedded = fieldDfn.embedded == true

        Map dfn = (Map) fieldDfn['linkRepeated']
        if (dfn) {
            linkRepeated = [
                    link        : dfn.addLink ?: dfn.link,
                    repeatable  : dfn.containsKey('addLink'),
                    resourceType: dfn.resourceType,
                    embedded    : dfn.embedded
            ]

        }
    }

    abstract ConvertResult convert(Map state, value)

    abstract def revert(Map data, Map result)

    Map getLinkRule(Map state, value) {
        if (linkRepeated) {
            Collection tagValues = (Collection) state.sourceMap[tag]
            if (tagValues.size() > 1 && !value.is(tagValues[0][tag])) {
                return linkRepeated
            }
        }
        return [
                link        : this.link,
                repeatable  : this.repeatable,
                resourceType: this.resourceType,
                embedded    : this.embedded
        ]
    }

}

@CompileStatic
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

@CompileStatic
class MarcFixedFieldHandler {

    String tag
    static final String FIXED_UNDEF = "|"
    static final String FIXED_NONE = " "
    static final String FIXED_FAUX_NONE = "_"
    List<Column> columns = []
    int fieldSize = 0

    MarcFixedFieldHandler(MarcRuleSet ruleSet, String tag, Map fieldDfn) {
        this.tag = tag
        fieldDfn?.each { key, obj ->
            def m = (key =~ /^\[(\d+):(\d+)\]$/)
            if (m && obj) {
                int start = m.group(1).toInteger()
                int end = m.group(2).toInteger()
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

    ConvertResult convert(Map state, value) {
        def success = true
        def failedFixedFields = ((Map) state.marcRemains).failedFixedFields
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

    @CompileStatic(SKIP)
    def revert(Map data, Map result, boolean keepEmpty = false) {
        def value = new StringBuilder(FIXED_NONE * fieldSize)
        def actualValue = false
        for (col in columns) {
            def obj = col.revert(data)
            // TODO: ambiguity trouble if this is a List!
            if (obj instanceof List) {
                obj = obj.find { it }
            }
            obj = (String) obj
            if (obj) {
                assert col.width - obj.size() > -1
                assert value.size() > col.start
                assert col.width >= obj.size()
                def end = col.start + obj.size() - 1
                value[col.start..end] = obj
                if (col.isActualValue(obj)) {
                    actualValue = true
                }
            }
        }
        return (actualValue || keepEmpty) ? value.toString() : null
    }

    class Column extends MarcSimpleFieldHandler {

        int start
        int end
        String fixedDefault
        Pattern matchAsDefault

        Column(ruleSet, fieldDfn, int start, int end,
               fixedDefault, matchAsDefault = null) {
            super(ruleSet, null, fieldDfn)
            assert start > -1 && end >= start
            this.start = start
            this.end = end
            this.fixedDefault = fixedDefault
            if (matchAsDefault) {
                this.matchAsDefault = Pattern.compile((String) matchAsDefault)
            }
            if (!fixedDefault && tokenMap &&
                    !tokenMap.containsKey(FIXED_FAUX_NONE) &&
                    !tokenMap.containsKey(FIXED_NONE)) {
                this.fixedDefault = FIXED_UNDEF
            }
        }

        int getWidth() { return end - start }


        String getToken(value) { return getToken((String) value) }

        String getToken(String value) {
            if (value.size() < start)
                return ""
            if (value.size() < end)
                return value.substring(start)
            return value.substring(start, end)
        }

        ConvertResult convert(Map state, value) {
            def token = getToken(value)
            if (token == "")
                return OK
            if (token == fixedDefault)
                return OK
            if (matchAsDefault && matchAsDefault.matcher(token).matches())
                return OK
            boolean isNothing = token.find {
                it != FIXED_NONE && it != FIXED_UNDEF
            } == null
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
    Map tokenNames = [:]

    TokenSwitchFieldHandler(ruleSet, tag, Map fieldDfn, tokenMapKey = 'tokenTypeMap') {
        super(ruleSet, tag, fieldDfn)
        assert !link || repeatable // this kind should always be repeatable if linked
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

    ConvertResult convert(Map state, value) {
        def token = getToken(state.sourceMap.leader, value)
        def converter = handlerMap[token]
        if (converter == null)
            return FAIL

        def linkRule = getLinkRule(state, value)

        def entityMap = state.entityMap
        if (linkRule.link) {
            def ent = entityMap[aboutEntityName]
            def newEnt = newEntity(state, linkRule.resourceType, null, linkRule.embedded)
            addValue(ent, linkRule.link, newEnt, linkRule.repeatable)
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
        def rootEntity = getEntity(data)
        // TODO: using rootEntity instead of data fails when reverting bib 008
        def entities = [data]
        if (link) {
            entities = rootEntity.get(link) ?: []
        }
        if (linkRepeated) {
            def entitiesFromRepeated = rootEntity.get(linkRepeated.link)
            if (entitiesFromRepeated) {
                entities += Util.asList(entitiesFromRepeated)
            }
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
            // TODO: revert column lists instead and skip if isActualValue is
            // false for all. Filter below is poor since both "_" and "0" are
            // meaningful in some places!
            if (value.find {
                it != MarcFixedFieldHandler.FIXED_NONE &&
                        it != MarcFixedFieldHandler.FIXED_UNDEF &&
                        it != "0"
            }) {
                values << value
            }
        }
        return values
    }

}

@CompileStatic
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

    @CompileStatic(SKIP)
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

    ConvertResult convert(Map state, value) {
        if (ignored || !(property || link))
            return

        def strValue = (String) value

        if (tokenMap) {
            def mapped = tokenMap[strValue] ?: tokenMap[strValue.toLowerCase()]
            if (mapped == null) {
                return new ConvertResult(tokenMap.containsKey(strValue))
            } else if (mapped == false) {
                return OK
            } else {
                value = mapped
                strValue = (String) value
            }
        }

        if (dateTimeFormat) {
            def givenValue = value
            try {
                Temporal dateTime = null
                if (defaultTime) {
                    if (missingCentury) {
                        strValue = (strValue[0..2] > "70" ? "19" : "20") + strValue
                    }
                    dateTime = ZonedDateTime.of(LocalDate.parse(
                            strValue, dateTimeFormat),
                            defaultTime, timeZone)
                } else if (timeZone) {
                    dateTime = LocalDateTime.parse(strValue, dateTimeFormat).atZone(timeZone)
                } else {
                    dateTime = ZonedDateTime.parse(strValue, dateTimeFormat)
                }
                value = DT_FORMAT.format(dateTime)
            } catch (DateTimeParseException e) {
                value = givenValue
            }
        } else if (parseZeroPaddedNumber && value) {
            try {
                value = Integer.parseInt(strValue.trim())
            } catch (NumberFormatException e) {
                ; // pass
            }
        }

        def ent = (Map) state.entityMap[aboutEntityName]
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
            if (!matchUriToken || matchUriToken.matcher(strValue).matches()) {
                ent['@id'] = uriTemplate.replace(URI_SLOT, strValue.trim())
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

    @CompileStatic(SKIP)
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
            return (entity instanceof List ? entity : [entity]).collect {
                def id = it instanceof Map ? it['@id'] : it
                if (uriTemplate) {
                    def token = extractToken(uriTemplate, id)
                    if (token) {
                        return revertObject(token)
                    }
                }
            }
        }
    }

    static ZonedDateTime parseDate(String s) {
        try {
            return ZonedDateTime.parse(s, DT_FORMAT)
        } catch (DateTimeParseException e) {
            return ZonedDateTime.parse(s, DT_FORMAT_FALLBACK)
        }
    }

    static String extractToken(String tplt, String value) {
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
@CompileStatic
class MarcFieldHandler extends BaseMarcFieldHandler {

    MarcSubFieldHandler ind1
    MarcSubFieldHandler ind2
    List<String> dependsOn
    Map<String, Map> constructProperties
    String uriTemplate
    Set<String> uriTemplateKeys
    Map uriTemplateDefaults
    Map computeLinks
    Map<String, MarcSubFieldHandler> subfields = [:]
    List<MatchRule> matchRules = []
    Map<String, Map> pendingResources
    String ignoreOnRevertInFavourOf

    static GENERIC_REL_URI_TEMPLATE = "generic:{_}"

    @CompileStatic(SKIP)
    MarcFieldHandler(ruleSet, tag, fieldDfn) {
        super(ruleSet, tag, fieldDfn)
        ind1 = fieldDfn.i1 ? new MarcSubFieldHandler(this, "ind1", fieldDfn.i1) : null
        ind2 = fieldDfn.i2 ? new MarcSubFieldHandler(this, "ind2", fieldDfn.i2) : null
        pendingResources = fieldDfn.pendingResources

        dependsOn = fieldDfn.dependsOn

        constructProperties = fieldDfn.constructProperties

        if (fieldDfn.uriTemplate) {
            uriTemplate = fieldDfn.uriTemplate
            uriTemplateKeys = fromTemplate(uriTemplate).variables as Set
            uriTemplateDefaults = fieldDfn.uriTemplateDefaults
        }
        ignoreOnRevertInFavourOf = fieldDfn.ignoreOnRevertInFavourOf

        computeLinks = (fieldDfn.computeLinks) ? new HashMap(fieldDfn.computeLinks) : [:]
        if (computeLinks) {
            computeLinks.use = computeLinks.use.replaceFirst(/^\$/, '')
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
                addSubfield(m.group(1), obj)
            }
        }
    }

    void addSubfield(String code, Map dfn) {
        subfields[code] = dfn ? new MarcSubFieldHandler(this, code, dfn) : null
    }

    ConvertResult convert(Map state, value) {
        if (!(value instanceof Map)) {
            throw new MalformedFieldValueException()
        }
        return convert(state, (Map) value)
    }

    ConvertResult convert(Map state, Map value) {

        Map<String, Map> entityMap = (Map) state.entityMap

        Map aboutEntity = entityMap[aboutEntityName]
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

        Map entity = aboutEntity

        def handled = new HashSet()

        def linkage = computeLinkage(state, entity, value, handled)
        if (linkage.newEntity != null) {
            entity = (Map) linkage.newEntity
        }

        def uriTemplateParams = [:]

        def unhandled = new HashSet()

        def localEntites = [:]

        [ind1: ind1, ind2: ind2].each { indKey, handler ->
            if (!handler)
                return
            def ok = handler.convertSubValue(state, value[indKey], entity,
                    uriTemplateParams, localEntites)
            if (!ok && handler.marcDefault == null) {
                unhandled << indKey
            }
        }

        value.subfields.each { Map it ->
            it.each { code, subVal ->
                def subDfn = (MarcSubFieldHandler) subfields[code]
                boolean ok = false
                if (subDfn) {
                    def entKey = subDfn.aboutEntityName
                    def ent = (Map) (entKey ? entityMap[entKey] : entity)
                    if ((subDfn.requiresI1 && subDfn.requiresI1 != value.ind1) ||
                            (subDfn.requiresI2 && subDfn.requiresI2 != value.ind2)) {
                        ok = true // rule does not apply here
                    } else {
                        ok = subDfn.convertSubValue(state, subVal, ent,
                                uriTemplateParams, localEntites)
                    }
                }
                if (!ok && !handled.contains(code)) {
                    unhandled << code
                }
            }
        }

        if (constructProperties) {
            constructProperties.each { key, dfn ->
                if (true) { //!key in entity) {
                    def parts = Util.getAllByPath(entity, (String) dfn.property)
                    if (parts) {
                        def constructed = parts.join((String) dfn.join)
                        entity[key] = constructed
                        uriTemplateParams[key] = constructed
                    }
                }
            }
        }

        if (uriTemplate) {
            uriTemplateKeys.each { String k ->
                if (!uriTemplateParams.containsKey(k)) {
                    def v = Util.getByPath(aboutEntity, k)
                    if (!v && uriTemplateDefaults) {
                        v = uriTemplateDefaults[k]
                    }
                    if (v) {
                        uriTemplateParams[k] = v
                    }
                }
            }
            // TODO: need to run before linking resource above to work properly
            // for multiply linked entities. Or, perhaps better, run a final "mapIds" pass
            // in the main loop..
            if (uriTemplateParams.keySet().containsAll(uriTemplateKeys)) {
                def computedUri = fromTemplate(uriTemplate).expand(uriTemplateParams)
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
        }

        return new ConvertResult(unhandled)
    }

    @CompileStatic(SKIP)
    Map computeLinkage(Map state, Map entity, Map value, Set handled) {
        def linkRule = getLinkRule(state, value)
        def newEnt = null
        if (linkRule.link || linkRule.resourceType) {
            def useLinks = Collections.emptyList()
            if (computeLinks) {
                def use = computeLinks.use
                Map resourceMap = (Map) ((computeLinks.mapping instanceof Map) ?
                        computeLinks.mapping : tokenMaps[computeLinks.mapping])
                def linkTokens = value.subfields.findAll { Map it ->
                    use in it.keySet()
                }.collect { ((Map.Entry) it.iterator().next()).value }
                useLinks = linkTokens.collect {
                    def linkDfn = resourceMap[it]
                    if (linkDfn == null) {
                        linkDfn = resourceMap[it.toLowerCase().replaceAll(/[^a-z0-9_-]/, '')]
                    }
                    if (linkDfn instanceof String)
                        return linkDfn
                    else
                        return fromTemplate(GENERIC_REL_URI_TEMPLATE).expand(["_": it])
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

            if (useLinks) {
                linkRule.repeatable = true
            }

            newEnt = newEntity(state, linkRule.resourceType, null, linkRule.embedded)

            // TODO: use @id (existing or added bnode-id) instead of duplicating newEnt
            def entRef = newEnt
            if (useLinks && linkRule.link) {
                if (!newEnt['@id'])
                    newEnt['@id'] = "_:b-${state.bnodeCounter++}" as String
                entRef = ['@id': newEnt['@id']]
            }
            if (linkRule.link) {
                addValue(entity, linkRule.link, newEnt, linkRule.repeatable)
            }
            useLinks.each {
                addValue(entity, it, entRef, linkRule.repeatable)
            }
        }
        return [ // TODO: just returning the entity would be enough
                 newEntity: newEnt
        ]
    }

    Map getLocalEntity(Map state, Map owner, String id, Map localEntities) {
        def entity = (Map) localEntities[id]
        if (entity == null) {
            def pending = pendingResources[id]
            entity = localEntities[id] = newEntity(state,
                    (String) pending.resourceType,
                    null, // id
                    (Boolean) pending.embedded)
            def link = (String) (pending.link ?: pending.addLink)
            if (pending.about) {
                owner = getLocalEntity(state, owner, (String) pending.about, localEntities)
            }
            addValue(owner, link, entity, pending.containsKey('addLink'))
        }
        return entity
    }

    @CompileStatic(SKIP)
    def revert(Map data, Map result, MatchCandidate matchCandidate = null) {

        if (ignoreOnRevertInFavourOf) {
            return null
        }

        def matchedResults = []

        if (matchCandidate == null) {
            // NOTE: The order of rules upon revert must be reverse, since that
            // incidentally runs the most independent and specific rules last.
            // TODO: MatchRule mechanics should be simplified!
            for (rule in matchRules.reverse(false)) {
                for (candidate in rule.candidates) {
                    // NOTE: Combinations allow for one nested level...
                    if (candidate.handler.matchRules) {
                        for (subrule in candidate.handler.matchRules) {
                            for (subcandidate in subrule.candidates) {
                                def matchres = subcandidate.handler.revert(data, result, subcandidate)
                                matchedResults += matchres
                            }
                        }
                    }
                    def matchres = candidate.handler.revert(data, result, candidate)
                    matchedResults += matchres
                }
            }
        }

        def entity = getEntity(data)

        def types = entity['@type']
        if (types instanceof String) {
            types = [types]
        }
        if (definesDomainEntityType && !(types.contains(definesDomainEntityType)))
            return null

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
            useLinks << [link: link, resourceType: resourceType]
        }

        if (linkRepeated) {
            useLinks << [link: linkRepeated.link, resourceType: linkRepeated.resourceType]
        }

        for (useLink in useLinks) {
            def useEntities = [entity]
            if (useLink.link) {
                useEntities = entity[useLink.link]
                if (!(useEntities instanceof List)) // should be if repeat == true
                    useEntities = useEntities ? [useEntities] : []
                if (useLink.resourceType) {
                    useEntities = useEntities.findAll {
                        if (!it) return false
                        def type = it['@type']
                        return (type instanceof List) ?
                                useLink.resourceType in type : type == useLink.resourceType
                    }
                }
            }

            def aboutMap = [:]
            if (pendingResources) {
                pendingResources.each { key, pending ->
                    def link = pending.link ?: pending.addLink
                    def resourceType = pending.resourceType
                    useEntities.each {
                        def parent = it
                        if (pending.about) {
                            def pendingParent = pendingResources[pending.about]
                            if (pendingParent) {
                                def parentLink = pendingParent.link ?: pendingParent.addLink
                                parent = it[parentLink]
                            }
                        }
                        def about = parent ? parent[link] : null
                        Util.asList(about).each {
                            if (it && (!resourceType || it['@type'] == resourceType)) {
                                aboutMap.get(key, []).add(it)
                            }
                        }
                    }
                }
            }

            aboutMap[null] = [null] // dummy to always enter loop... (refactor time...)
            aboutMap.each { key, abouts ->
                abouts.each { about ->
                    def oneAboutMap = key ? [(key): about] : [:]
                    useEntities.each {
                        def field = revertOne(data, it, null, oneAboutMap, matchCandidate)
                        if (field) {
                            if (useLink.subfield) {
                                field.subfields << useLink.subfield
                            }
                            results << field
                        }
                    }
                }
            }
        }

        return results + matchedResults
    }

    @CompileStatic(SKIP)
    def revertOne(Map data, Map currentEntity, Set onlyCodes = null, Map aboutMap = null,
                  MatchCandidate matchCandidate = null) {

        def i1 = matchCandidate?.ind1 ?: ind1 ? ind1.revert(data, currentEntity) : ' '
        def i2 = matchCandidate?.ind2 ?: ind2 ? ind2.revert(data, currentEntity) : ' '

        def subs = []
        def failedRequired = false

        def selectedEntities = new HashSet()
        def thisTag = this.tag.split(':')[0]

        subfields.each { code, subhandler ->
            if (failedRequired)
                return

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
            def selectedEntity = subhandler.about ? aboutMap[subhandler.about] : currentEntity
            if (!selectedEntity)
                return

            if (selectedEntity != currentEntity && selectedEntity._revertedBy == thisTag) {
                failedRequired = true
                //return
            }

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

            if (!failedRequired) {
                selectedEntities << selectedEntity
            }
        }

        if (!failedRequired && i1 != null && i2 != null && subs.length) {
            // FIXME: store reverted input refs instead of tagging input data
            selectedEntities.each {
                it._revertedBy = thisTag
            }
            return [ind1: i1, ind2: i2, subfields: subs]
        } else {
            return null
        }
    }
}

@CompileStatic
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
    String subUriTemplate
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
    // TODO: itemPos is not used right now. Only supports first/rest.
    String itemPos

    @CompileStatic(SKIP)
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
            subUriTemplate = subDfn.uriTemplate
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

    boolean convertSubValue(Map state, def subVal, Map ent,
                            Map uriTemplateParams, Map localEntites) {
        def ok = false
        String uriTemplateKeyBase = ""

        if (subVal)
            subVal = clearChars((String) subVal)
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
            String entId = null
            if (subUriTemplate) {
                try {
                    entId = fromTemplate(subUriTemplate).expand(["_": subVal])
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
            def m = splitValuePattern.matcher((String) subVal)
            if (m) {
                splitValueProperties.eachWithIndex { prop, i ->
                    def v = m.group(i + 1)
                    if (v) {
                        ent[prop] = v
                    }
                    fieldHandler.addValue(uriTemplateParams,
                            uriTemplateKeyBase + prop, v, true)
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

    String clearChars(String subVal) {
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

    @CompileStatic(SKIP)
    def revert(Map data, Map currentEntity) {
        currentEntity = aboutEntityName ? getEntity(data) : currentEntity
        if (currentEntity == null)
            return null

        def entities = link ? currentEntity[link] : [currentEntity]
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
                    // NOTE: requires variable slot to be at end of template
                    // TODO: unify with extractToken
                    def exprIdx = subUriTemplate.indexOf('{_}')
                    if (exprIdx > -1) {
                        assert subUriTemplate.size() == exprIdx + 3
                        obj = URLDecoder.decode(obj.substring(exprIdx))
                    } else {
                        exprIdx = subUriTemplate.indexOf('{+_}')
                        if (exprIdx > -1) {
                            assert subUriTemplate.size() == exprIdx + 4
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

    boolean combinatory = false

    MatchRule(MarcFieldHandler handler, Map fieldDfn, String ruleKey, rules) {
        rules.each { String key, Map matchDfn ->
            def comboDfn = (Map) fieldDfn.clone()
            if (!combinatory) {
                comboDfn.remove(ruleKey)
            }
            // create nested combinations, but prevent recursive nesting
            if (comboDfn.nestedMatch) {
                matchRuleKeys.each {
                    comboDfn.remove(it)
                }
            } else {
                comboDfn.nestedMatch = true
                if (combinatory) {
                    def newRule = (Map) comboDfn[ruleKey].clone()
                    newRule.remove(key)
                    comboDfn[ruleKey] = newRule
                }
            }
            comboDfn += matchDfn
            def tag = "${handler.tag}:${ruleKey}:${key}"
            ruleMap[key] = new MarcFieldHandler(handler.ruleSet, tag, comboDfn)
        }
    }

    @CompileStatic
    MarcFieldHandler getHandler(Map entity, value) {
        for (String key : getKeys(entity, value)) {
            def handler = (MarcFieldHandler) ruleMap[key]
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

@CompileStatic
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
        return types instanceof String ? [types] : types ?: []
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

    boolean combinatory = true

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

@CompileStatic
class Util {

    static List asList(o) {
        return (o instanceof List) ? (List) o : o != null ? [o] : []
    }

    static String getByPath(Map entity, String path) {
        return getAllByPath(entity, path)[0]
    }

    static List getAllByPath(entity, String path) {
        def results = []
        collectByChain(entity, path.split(/\./) as List, results)
        return results
    }

    static void collectByChain(entity, List<String> chain, List results) {
        asList(entity).each {
            def value = it[chain[0]]
            if (chain.size() > 1) {
                collectByChain(value, chain.subList(1, chain.size()), results)
            } else {
                results.addAll(asList(value))
            }
        }
    }

}

class MalformedFieldValueException extends RuntimeException {}

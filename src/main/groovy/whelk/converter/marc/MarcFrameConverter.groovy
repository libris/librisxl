package whelk.converter.marc

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.converter.FormatConverter
import whelk.filter.LinkFinder
import whelk.util.PropertyLoader
import whelk.util.URIWrapper

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
    LinkFinder linkFinder

    protected MarcConversion conversion

    MarcFrameConverter(LinkFinder linkFinder = null) {
        this.linkFinder = linkFinder
        def config = readConfig("$cfgBase/marcframe.json")
        initialize(config)
    }

    private MarcFrameConverter(Map config) {
        initialize(config)
    }

    Map readConfig(String path) {
        return getClass().classLoader.getResourceAsStream(path).withStream {
            mapper.readValue(it, SortedMap)
        }
    }

    void initialize(Map config) {
        def tokenMaps = loadTokenMaps(config.tokenMaps)
        conversion = new MarcConversion(this, config, tokenMaps)
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
                    maps += readConfig("$cfgBase/$it")
                } else {
                    maps += it
                }
            }
        } else {
            maps = tokenMaps
        }
        maps.each { key, src ->
            if (src instanceof String) {
                result[key] = readConfig("$cfgBase/$src")
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
        def extraData = [:]

        for (fpath in fpaths) {
            def source = converter.mapper.readValue(new File(fpath), Map)
            def result = null
            if (cmd == "revert") {
                result = converter.runRevert(source)
            } else {
                result = converter.runConvert(source, fpath, extraData)
            }
            if (fpaths.size() > 1)
                println "SOURCE: ${fpath}"
            try {
                println converter.mapper.writeValueAsString(result)
            } catch (e) {
                System.err.println "Error in result:"
                System.err.println result
                throw e
            }
        }
    }

}

@Log
class MarcConversion {

    static MARC_CATEGORIES = ['bib', 'auth', 'hold']

    MarcFrameConverter converter
    List<MarcFramePostProcStep> sharedPostProcSteps
    Map<String, MarcRuleSet> marcRuleSets = [:]
    boolean doPostProcessing = true
    boolean flatLinkedForm = true
    Map marcTypeMap = [:]
    Map tokenMaps

    URIWrapper baseUri = Document.BASE_URI

    MarcConversion(MarcFrameConverter converter, Map config, Map tokenMaps) {
        marcTypeMap = config.marcTypeFromTypeOfRecord
        this.tokenMaps = tokenMaps
        this.converter = converter
        //this.baseUri = new URI(config.baseUri ?: '/')

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
            case 'VerboseRevertData': new VerboseRevertDataStep(props); break
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

        // NOTE: use minted ("pretty") uri as primary @id
        if (recordId != null) {
            if (record["@id"]) {
                record.get('sameAs', []) << ['@id': resolve(recordId)]
            } else {
                record["@id"] = resolve(recordId)
            }
        }

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

        if (flatLinkedForm) {
            return toFlatLinkedForm(state, marcRuleSet, extraData)
        } else {
            return record
        }
    }

    def toFlatLinkedForm(state, marcRuleSet, extraData) {
        marcRuleSet.topPendingResources.each { key, dfn ->
            if (dfn.about && dfn.link && !dfn.embedded) {
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

        def quotedIds = new HashSet()

        if (converter.linkFinder) {
            def recordCandidates = extraData?.get("oaipmhSetSpecs")?.findResults {
                def (spec, authId) = it.split(':')
                if (spec != 'authority')
                    return
                fromTemplate(marcRuleSet.topPendingResources['?record'].uriTemplate).expand(
                        marcType: 'auth', controlNumber: authId)
            }
            def newUris = converter.linkFinder.findLinks(state.quotedEntities, recordCandidates)
            state.quotedEntities.eachWithIndex { ent, i ->
                def newUri = newUris[i]
                if (newUri) {
                    ent.clear()
                    ent['@id'] = newUri.toString()
                }
            }
        }
        else {
            log.debug "No linkfinder present"
        }

        ArrayList entities = state.entityMap.findResults { key, ent ->
            if (!marcRuleSet.topPendingResources[key].embedded) ent
        }
        return [
                '@graph': entities
        ]
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

    Map<String, Map> topPendingResources = [:]

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

            if (dfn.ignored || dfn.size() == 0) {
                return
            }

            thingLink = topPendingResources['?thing'].link
            definingTrait = topPendingResources['?work']?.link

            dfn = processInherit(config, subConf, tag, dfn)

            dfn = processInclude(config, dfn, tag)

            // aboutTypeMap is used on revert to determine which ruleSet to use
            if (dfn.aboutType && dfn.aboutType != 'Record') {
                if (dfn.aboutEntity) {
                    assert tag && aboutTypeMap.containsKey(dfn.aboutEntity)
                }
                aboutTypeMap[dfn.aboutEntity ?: '?thing'] << dfn.aboutType
            }
            for (matchDfn in dfn['match']) {
                if (matchDfn.aboutType && matchDfn.aboutType != 'Record') {
                    aboutTypeMap[matchDfn.aboutEntity ?: dfn.aboutEntity ?: '?thing'] << matchDfn.aboutType
                }
            }

            def handler = null
            if (dfn.tokenTypeMap) {
                handler = new TokenSwitchFieldHandler(this, tag, dfn)
            } else if (dfn.recTypeBibLevelMap) {
                handler = new TokenSwitchFieldHandler(this, tag, dfn, 'recTypeBibLevelMap')
            } else if (dfn.find { it.key[0] == '[' }) {
                handler = new MarcFixedFieldHandler(this, tag, dfn)
            } else if ('match' in dfn || dfn.find { it.key[0] == '$' }) {
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
        def merged = [:]

        def includes = fieldDfn['include']

        if (includes) {
            for (include in Util.asList(includes)) {
                def baseDfn = config['patterns'][include]
                assert tag && baseDfn
                if (baseDfn.include) {
                    baseDfn = processInclude(config, baseDfn, tag)
                }
                assert tag && !(baseDfn.keySet().intersect(fieldDfn.keySet()) - 'include')
                merged += baseDfn
            }
        }

        merged += fieldDfn
        merged.remove('include')

        def matchRules = merged['match']
        if (matchRules) {
            merged['match'] = matchRules.collect {
                processInclude(config, it, tag)
            }
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
                def uri = dfn.uriTemplate == '{+_}' ?
                    value
                    : conversion.resolve(fromTemplate(
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
                    // Merge if there already is one (and only one) linked
                    if (existing instanceof Map) {
                        entity.each { k, v ->
                            if (existing.containsKey(k)) {
                                existing[k] = Util.asList(existing[k]) + [v]
                            } else {
                                existing[k] = v
                            }
                        }
                    } else if (existing instanceof List) {
                        existing << entity
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
    String tokenMapName // TODO: remove in columns in favour of @type+code/uriTemplate ?
    Map reverseTokenMap
    boolean embedded = false

    void setTokenMap(BaseMarcFieldHandler fieldHandler, Map dfn) {
        def tokenMap = dfn.tokenMap
        if (tokenMap) {
            if (tokenMap instanceof String)
                tokenMapName = tokenMap
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
    Map computeLinks
    boolean repeatable = false
    String resourceType
    Map linkRepeated = null
    boolean onlySubsequentRepeated = false

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

        Map dfn = (Map) fieldDfn['linkEveryIfRepeated']
        if (fieldDfn['linkSubsequentRepeated']) {
            assert !dfn, "linkEveryIfRepeated and linkSubsequentRepeated not allowed on ${fieldId}"
            dfn = (Map) fieldDfn['linkSubsequentRepeated']
            onlySubsequentRepeated = true
        }
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

    String getFieldId() {
        return "${ruleSet.name} ${tag}"
    }

    Map getLinkRule(Map state, value) {
        if (linkRepeated) {
            Collection tagValues = (Collection) state.sourceMap[tag]
            if (tagValues.size() > 1) {
                boolean firstOccurrence = value.is(tagValues[0][tag])
                if (!(firstOccurrence && onlySubsequentRepeated)) {
                    return linkRepeated
                }
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

    MarcRuleSet ruleSet
    String tag
    static final String FIXED_UNDEF = "|"
    static final String FIXED_NONE = " "
    static final String FIXED_FAUX_NONE = "_"
    List<Column> columns = []
    int fieldSize = 0

    MarcFixedFieldHandler(MarcRuleSet ruleSet, String tag, Map fieldDfn) {
        this.ruleSet = ruleSet
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
            assert value.size() > col.start // columns must fit within value
            def obj = col.revert(data)
            // TODO: ambiguity trouble if this is a List!
            if (obj instanceof List) {
                obj = obj.find { it && col.width >= it.size() }
            }
            obj = (String) obj
            if (obj && col.width >= obj.size()) {
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
            if (fixedDefault) {
                assert this.fixedDefault.size() == this.width
            }
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
            if ((v == null || v.every { it == null }) && fixedDefault)
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
    Map<String, MarcSubFieldHandler> subfields = [:]
    List<MatchRule> matchRules
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

        matchRules = MatchRule.parseRules(this, fieldDfn) ?: Collections.emptyList()

        def aboutAlias = fieldDfn['about']

        fieldDfn.each { key, obj ->
            def m = key =~ /^\$(\w+)$/
            if (m) {
                if (obj && obj['about'] == aboutAlias) {
                    obj = obj.findAll { it.key != 'about' }
                }
                addSubfield(m.group(1), obj)
            }
        }

        assert !resourceType || link || computeLinks != null, "Expected link on $fieldId with resourceType: $resourceType"
        assert !embedded || link || computeLinks != null, "Expected link on embedded $fieldId"
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
            def matchHandler = rule.getHandler(aboutEntity, value)
            if (matchHandler) {
                return matchHandler.convert(state, value)
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
                def subDfn = (MarcSubFieldHandler) subfields[code as String]
                boolean ok = false
                if (subDfn) {
                    def entKey = subDfn.aboutEntityName
                    def ent = (Map) (entKey ? entityMap[entKey] : entity)
                    if ((subDfn.requiresI1 && subDfn.requiresI1 != value.ind1) ||
                            (subDfn.requiresI2 && subDfn.requiresI2 != value.ind2)) {
                        ok = true // rule does not apply here
                    } else {
                        // TODO: if codePatternReset, aboutNew: true
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
                // NOTE: We used to force minted ("pretty") uri to be an alias here...
                /*if (definesDomainEntityType != null) {
                    addValue(entity, altUriRel, ['@id': computedUri], true)
                } else {*/
                if (entity['@id']) {
                    // TODO: ok as precaution?
                    addValue(entity, altUriRel, ['@id': entity['@id']], true)
                }
                // ... but now we promote it to primary id if none has been given.
                entity['@id'] = computedUri
                /*}*/
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

    Map getLocalEntity(Map state, Map owner, String id, Map localEntities, boolean forceNew = false) {
        def entity = (Map) localEntities[id]
        if (entity == null || forceNew) {
            assert pendingResources, "Missing pendingResources in ${fieldId}, cannot use ${id}"
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
    def revert(Map data, Map result, MatchRule usedMatchRule = null) {

        if (ignoreOnRevertInFavourOf) {
            return null
        }

        def matchedResults = []

        for (rule in matchRules) {
            def matchres = rule.handler.revert(data, result, rule)
            if (matchres) {
                matchedResults += matchres
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
                useEntities = Util.asList(entity[useLink.link])
                if (useLink.resourceType) {
                    useEntities = useEntities.findAll {
                        if (!it) return false
                        assert it instanceof Map, "Error reverting ${fieldId} - expected object, got: ${it}"
                        def type = it['@type']
                        return (type instanceof List) ?
                                useLink.resourceType in type : type == useLink.resourceType
                    }
                }
            }

            useEntities.each {
                def field = revertOne(data, it, buildAboutMap(it), usedMatchRule)
                if (field) {
                    if (useLink.subfield) {
                        field.subfields << useLink.subfield
                    }
                    results << field
                }
            }
        }

        return results + matchedResults
    }

    @CompileStatic(SKIP)
    Map<String, List> buildAboutMap(Map entity) {
        Map<String, List> aboutMap = [:]
        if (!pendingResources) {
            return aboutMap
        }
        pendingResources.each { key, pending ->
            def link = pending.link ?: pending.addLink
            def resourceType = pending.resourceType

            def parent = entity
            if (pending.about) {
                def pendingParent = pendingResources[pending.about]
                // TODO: ensure nested pending work (any depth?)
                if (pendingParent) {
                    def parentLink = pendingParent.link ?: pendingParent.addLink
                    parent = entity[parentLink]
                }
            }
            def about = parent ? parent[link] : null
            Util.asList(about).each {
                if (it && (!resourceType || it['@type'] == resourceType)) {
                    aboutMap.get(key, []).add(it)
                }
            }
        }
        return aboutMap
    }

    @CompileStatic(SKIP)
    def revertOne(Map data, Map currentEntity, Map<String, List> aboutMap = null,
                    MatchRule usedMatchRule = null) {

        def i1 = usedMatchRule?.ind1 ?: ind1 ? ind1.revert(data, currentEntity) : ' '
        def i2 = usedMatchRule?.ind2 ?: ind2 ? ind2.revert(data, currentEntity) : ' '

        def subs = []
        def failedRequired = false

        def usedEntities = new HashSet()
        def thisTag = this.tag.split(':')[0]

        Map<String, List> remainingAboutMap = [:]

        // TODO: exhaust local pending accordingly...
        def subhandlersByAbout = subfields.values().findAll { it }.groupBy { it.about }.entrySet()

        subhandlersByAbout.sort { it.key != null }.each { it ->

            def about = it.key
            def subhandlers = it.value

            def subhandlersByEntity
            if (about == null) {
                subhandlersByEntity = subhandlers.collect { [it, currentEntity ] }
            } else {
                def selectedEntities = aboutMap[about]
                subhandlersByEntity = []
                selectedEntities.each { entity ->
                    subhandlers.each { subhandlersByEntity << [it, entity ] }
                }
            }

            subhandlersByEntity.each { MarcSubFieldHandler subhandler, Map selectedEntity ->
                def code = subhandler.code
                if (failedRequired)
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

                if (!selectedEntity) {
                    failedRequired = true
                    return
                }

                if (selectedEntity != currentEntity && selectedEntity._revertedBy == thisTag) {
                    failedRequired = true
                    return
                }

                def value = subhandler.revert(data, selectedEntity)
                if (value instanceof List) {
                    value.each {
                        if (!usedMatchRule || usedMatchRule.matchValue(code, it)) {
                            subs << [(code): it]
                        }
                    }
                } else if (value != null) {
                    if (!usedMatchRule || usedMatchRule.matchValue(code, value)) {
                        subs << [(code): value]
                    }
                } else {
                    if (subhandler.required || subhandler.requiresI1 || subhandler.requiresI2) {
                        failedRequired = true
                    }
                }
                if (!failedRequired) {
                    usedEntities << selectedEntity
                }
            }
        }

        if (!failedRequired && i1 != null && i2 != null && subs.size()) {
            // FIXME: store reverted input refs instead of tagging input data
            // TODO: if it._localPending, it._exhausted = true (do not use again in selectedEntities above)
            usedEntities.each { it._revertedBy = thisTag }
            //currentEntity._revertedBy = thisTag
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
    char[] punctuationChars
    char[] surroundingChars
    String link
    String about
    boolean newAbout
    boolean repeatable
    String property
    boolean repeatProperty
    boolean overwrite
    String resourceType
    String subUriTemplate
    Pattern splitValuePattern
    List<String> splitValueProperties
    String rejoin
    boolean allowEmpty
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
        punctuationChars = subDfn.punctuationChars?.toCharArray()
        surroundingChars = subDfn.surroundingChars?.toCharArray()
        super.setTokenMap(fieldHandler, subDfn)
        link = subDfn.link

        if (subDfn.aboutNew) {
            about = subDfn.aboutNew
            newAbout = true
        } else {
            about = subDfn.about
        }

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
        overwrite = subDfn.overwrite == true
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
        marcDefault = subDfn.marcDefault
        definedElsewhereToken = subDfn.definedElsewhereToken
        requiresI1 = subDfn['requires-i1']
        requiresI2 = subDfn['requires-i2']
        itemPos = subDfn.itemPos

        assert !resourceType || link, "Expected link on ${fieldHandler.fieldId}-$code"
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
            ent = fieldHandler.getLocalEntity(state, ent, about, localEntites, newAbout)
        }

        if (link) {
            String entId = null
            if (subUriTemplate) {
                try {
                    entId = subUriTemplate == '{+_}' ?
                        subVal : fromTemplate(subUriTemplate).expand(["_": subVal])
                } catch (IllegalArgumentException|IndexOutOfBoundsException e) {
                    // Bad characters in what should have been a proper URI path ('+' expansion).
                    ; // TODO: We just drop the attempt here if the uriTemplate fails...
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
            if (overwrite) {
                ent[property] = subVal
            } else {
                fieldHandler.addValue(ent, property, subVal, repeatProperty)
            }
            fieldHandler.addValue(uriTemplateParams, uriTemplateKeyBase + property, subVal, true)
            ok = true
        }

        return ok
    }

    String clearChars(String val) {
        if (val.size() > 2) {
            val = val.trim()
            if (punctuationChars) {
                for (c in punctuationChars) {
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

@CompileStatic
class MatchRule {

    static List<MatchRule> parseRules(MarcFieldHandler parent, Map fieldDfn) {
        List<Map> matchDefs = (List) fieldDfn.remove('match')
        if (matchDefs.is(null))
            return null

        Map<String, Map> ruleWhenMap = matchDefs.collectEntries {
            [it['when'], it]
        }
        Map dfnCopy = fieldDfn.findAll { it.key != 'match' }
        return matchDefs?.collect {
            new MatchRule(parent, dfnCopy + (Map) it, ruleWhenMap)
        }
    }

    MarcFieldHandler handler
    boolean whenAll = true
    List<Closure> whenTests = []

    // TODO: change to use parsedWhen results to also do the revert check?
    String ind1
    String ind2

    Map<String, Closure> codePatterns = [:]

    String matchDomain

    boolean matchValue(String code, String value) {
        Closure pattern = codePatterns[code]
        if (!pattern)
            return true
        return pattern(value)
    }

    MatchRule(MarcFieldHandler parent, Map dfn, Map<String, Map> ruleWhenMap) {
        String when = dfn.remove('when')
        parseWhen(parent.fieldId, when)
        String inherit = dfn.remove('inherit-match')
        if (inherit) {
            dfn = ruleWhenMap[inherit] + dfn
            dfn.remove('when')
            dfn.remove('inherit-match')
        }
        String tag = parent.tag
        if (when) {
            tag += "[${when}]"
        }
        handler = new MarcFieldHandler(parent.ruleSet, tag, dfn)
    }

    private Pattern whenPattern = ~/(?:(?:\$(\w+)|i(1|2)|(\S+))(?:\s*(=~?)\s*(\S+))?)(?:\s*(\&|\|)\s*)?/

    @CompileStatic(SKIP)
    private void parseWhen(String fieldId, String when) {
        if (when.is(null)) {
            whenTests << { true }
            return
        }
        def currentAndOrOr = null
        whenPattern.matcher(when).each {
            def (match, code, indNum, term, comparator, matchValue, andOrOr) = it
            if (andOrOr) {
                if (currentAndOrOr)
                    assert currentAndOrOr == andOrOr, "Unsupported combination of AND/OR in ${fieldId}[${when}]"
                whenAll = andOrOr == '&'
                currentAndOrOr = andOrOr
            }
            if (code) {
                if (comparator) {
                    Closure check = null
                    if (comparator == '=~') {
                        def pattern = Pattern.compile(matchValue)
                        check = codePatterns[code] = { pattern.matcher(it).matches() }
                    } else {
                        check = codePatterns[code] = { it == matchValue }
                    }
                    whenTests << { value ->
                        for (sub in value.subfields) {
                            if (code in sub) return check(sub[code])
                        }
                        return false
                    }
                } else {
                    whenTests << { value ->
                        for (sub in value.subfields) {
                            if (code in sub) return true
                        }
                        return false
                    }
                }
            } else if (indNum) {
                String indKey = "ind${indNum}"
                whenTests << { value -> value[indKey] == matchValue }
                if (indNum == '1')
                    ind1 = matchValue
                else if (indNum == '2')
                    ind2 = matchValue
            } else if (term == '@type') {
                matchDomain = matchValue
            }
        }
    }

    MarcFieldHandler getHandler(Map entity, value) {
        if (matchDomain) {
            if (entity['@type'] != matchDomain)
                return null
            else if (whenTests.size() == 0)
                return handler
        }
        if (whenAll) {
            if (whenTests.every { it(value) }) {
                return handler
            }
        } else if (whenTests.any { it(value) }) {
            return handler
        }
        return null
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

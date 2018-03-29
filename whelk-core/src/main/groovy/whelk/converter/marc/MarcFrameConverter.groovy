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
    JsonLd ld

    protected MarcConversion conversion

    MarcFrameConverter(LinkFinder linkFinder = null, JsonLd ld = null) {
        this.linkFinder = linkFinder
        setLd(ld)
    }

    void setLd(JsonLd ld) {
        this.ld = ld
        if (ld) initialize()
    }

    void initialize() {
        if (conversion) return
        initialize(readConfig("$cfgBase/marcframe.json"))
    }

    void initialize(Map config) {
        def tokenMaps = loadTokenMaps(config.tokenMaps)
        conversion = new MarcConversion(this, config, tokenMaps)
    }

    Map readConfig(String path) {
        return getClass().classLoader.getResourceAsStream(path).withStream {
            mapper.readValue(it, SortedMap)
        }
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
        initialize()
        return conversion.convert(marcSource, recordId, extraData)
    }

    Map runRevert(Map data) {
        initialize()
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

}

@Log
class MarcConversion {

    static MARC_CATEGORIES = ['bib', 'auth', 'hold']
    static Map<String, Integer> ENTITY_ORDER = ['?record': 0, '?thing': 1]

    MarcFrameConverter converter
    List<MarcFramePostProcStep> sharedPostProcSteps
    Map<String, MarcRuleSet> marcRuleSets = [:]
    boolean doPostProcessing = true
    boolean flatLinkedForm = true
    boolean keepGroupIds = false
    Map marcTypeMap = [:]
    Map tokenMaps

    private Set missingTerms = [] as Set
    private Set badRepeats = [] as Set

    URI baseUri = Document.BASE_URI

    MarcConversion(MarcFrameConverter converter, Map config, Map tokenMaps) {
        marcTypeMap = config.marcTypeFromTypeOfRecord
        this.tokenMaps = tokenMaps
        this.converter = converter
        //this.baseUri = new URI(config.baseUri ?: '/')
        this.keepGroupIds = config.keepGroupIds == true

        this.sharedPostProcSteps = config.postProcessing.collect {
            parsePostProcStep(it)
        }

        MARC_CATEGORIES.each { marcCat ->
            def marcRuleSet = new MarcRuleSet(this, marcCat)
            marcRuleSets[marcCat] = marcRuleSet
            marcRuleSet.buildHandlers(config)
        }
        addTypeMaps()

        // log.warn? Though, this "should" not happen in prod...
        missingTerms.each {
            System.err.println "Missing: $it.term a $it.type"
        }
        badRepeats.each {
            if (it.term.startsWith(converter.ld.expand('marc:')))
                return
            if (it.shouldRepeat)
                System.err.println "Expected to be repeatable: $it.term"
            else
                System.err.println "Unexpected repeat of: $it.term"
        }
    }

    void checkTerm(String key, String type, boolean repeatable) {
        if (!key || key == '@type' || !converter.ld)
            return
        def terms = converter.ld.vocabIndex.keySet()
        String term = converter.ld.expand((String) key)
        if (!(term in terms)) {
            missingTerms << [term: term, type: type]
        }
        def shouldRepeat = key in converter.ld.repeatableTerms
        if (repeatable != shouldRepeat) {
            badRepeats << [term: term, shouldRepeat: shouldRepeat]
        }
    }

    MarcFramePostProcStep parsePostProcStep(Map stepDfn) {
        def props = stepDfn.clone()
        for (k in stepDfn.keySet())
            if (k[0] == '_')
                props.remove(k)
        switch (stepDfn.type) {
            case 'FoldLinkedProperty': new FoldLinkedPropertyStep(props); break
            case 'FoldJoinedProperties': new FoldJoinedPropertiesStep(props); break
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

        URI recordUri = record["@id"] ? new URI(record["@id"]) : null
        state.quotedEntities.each {
            def entityId = it['@id']
            // TODO: Prepend groupId with prefix to avoid collision with
            // other possible uses of relative id:s?
            if (entityId && (entityId.startsWith('#') || entityId.startsWith('_:'))) {
                if (keepGroupIds && recordUri) {
                    it['@id'] = recordUri.resolve(entityId).toString()
                } else {
                    it.remove('@id')
                }
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

        ArrayList entities = []

        state.entityMap.entrySet().sort {
            ENTITY_ORDER.get(it.key, ENTITY_ORDER.size())
        }.findResults {
            if (!marcRuleSet.topPendingResources[it.key].embedded) {
                entities << it.value
            }
        }

        return ['@graph': entities]
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
        marcRuleSet.revertFieldOrder.each { tag ->
            def handler = marcRuleSet.fieldHandlers[tag]
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

        // Order fields in result by number, not by revertFieldOrder.
        fields.sort { it.keySet()[0] }

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

    Iterable<String> revertFieldOrder = new LinkedHashSet<>()

    // aboutTypeMap is used on revert to determine which ruleSet to use
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
            } else if (dfn.find(MarcFixedFieldHandler.&isColKey)) {
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

        String defaultThingType = topPendingResources['?thing']?.resourceType
        if (defaultThingType) {
            aboutTypeMap['?thing'] << defaultThingType
        }

        // Process onRevertPrefer to do two things:
        // 1. Build the revertFieldOrder to ensure the most prefered field is
        //    reverted first.
        // 2. Populate sharesGroupIdWith (see comment below).
        fieldHandlers.each { tag, handler ->
            if (handler instanceof MarcFieldHandler && handler.onRevertPrefer) {
                Collection tagsToPrefer = handler.onRevertPrefer.findAll {
                    it in fieldHandlers
                }
                tagsToPrefer.each {
                    def prefHandler = fieldHandlers[it]
                    if (prefHandler instanceof MarcFieldHandler && prefHandler.onRevertPrefer) {
                        assert !(tag in prefHandler.onRevertPrefer),
                                "$name $it onRevertPrefer conflicts with $tag"
                    }
                    // Populate sharesGroupIdWith to allow fields to share a
                    // groupId (to be combined with other fields), but exclude
                    // similar fields which are to be prefered on revert (see
                    // use of sharesGroupIdWith in revert).
                    if (handler.groupId && handler.groupId == prefHandler.groupId) {
                        prefHandler.sharesGroupIdWith = handler.sharesGroupIdWith
                        prefHandler.sharesGroupIdWith << tag
                        prefHandler.sharesGroupIdWith << it
                    }
                }
                revertFieldOrder += tagsToPrefer
            }
            if (!(tag in revertFieldOrder)) {
                revertFieldOrder << tag
            }
        }

        // DEBUG:
        //fieldHandlers.each { tag, handler ->
        //    if (handler instanceof MarcFieldHandler && handler.sharesGroupIdWith)
        //        System.err.println "$tag: $handler.sharesGroupIdWith"
        //}

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
                    baseDfn = processInclude(config, baseDfn, "$tag+$include")
                }
                mergeFieldDefinitions(baseDfn, merged, "$tag+$include")
            }
        }

        mergeFieldDefinitions(fieldDfn, merged, tag)
        merged.remove('include')

        def matchRules = merged['match']
        if (matchRules) {
            merged['match'] = matchRules.collect {
                processInclude(config, it, tag)
            }
        }

        return merged
    }

    static void mergeFieldDefinitions(Map sourceDfn, Map targetDfn, String tag,
            boolean preventOverwrites = true) {
        def targetPending = targetDfn.remove('pendingResources')
        if (preventOverwrites) {
            assert tag && !(sourceDfn.keySet().intersect(targetDfn.keySet())
                    - 'include' - 'NOTE' - 'TODO')
        }

        // Treat pendingResources specially by merging them.
        def sourcePending = sourceDfn.pendingResources
        if (sourcePending) {
            targetPending = targetPending ?: [:]
            //assert tag && !(targetPending.keySet().intersect(sourcePending.keySet()))
            targetPending.putAll(sourcePending)
        }

        targetDfn.putAll(sourceDfn)

        if (targetPending) {
            targetDfn.pendingResources = targetPending
        }
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


@Log
@CompileStatic
class ConversionPart {

    MarcRuleSet ruleSet
    String aboutEntityName
    Map tokenMap
    String tokenMapName // TODO: remove in columns in favour of @type+code/uriTemplate ?
    Map reverseTokenMap
    boolean embedded = false

    JsonLd getLd() {
        return ruleSet.conversion.converter.ld
    }

    String propTerm(key, boolean repeatable) {
        return term((String) key, 'DatatypeProperty', repeatable)
    }

    String linkTerm(key, boolean repeatable) {
        return term((String) key, 'ObjectProperty', repeatable)
    }

    String typeTerm(key) {
        return term((String) key, 'Class', false)
    }

    String term(String key, String type, boolean repeatable) {
        ruleSet.conversion.checkTerm(key, type, repeatable)
        return key
    }

    void setTokenMap(BaseMarcFieldHandler fieldHandler, Map dfn) {
        def tokenMap = dfn.tokenMap
        if (tokenMap) {
            if (tokenMap instanceof String)
                tokenMapName = tokenMap
            reverseTokenMap = [:]
            if (tokenMap instanceof String) {
                assert fieldHandler.tokenMaps.containsKey(tokenMap)
                this.tokenMap = (Map) fieldHandler.tokenMaps[tokenMap]
            } else {
                this.tokenMap = (Map) tokenMap
            }
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
                    // TODO:
                    // - check if overwritten @type is on the same
                    //   hierarchical path as the new, use most specific
                    ((Map) existing).each { k, v ->
                        if (v instanceof List) {
                            v.each {
                                addValue((Map) value, (String) k, it, true)
                            }
                        } else {
                            addValue((Map) value, (String) k, v, false)
                        }
                    }
                    l[l.indexOf(existing)] = value
                } else {
                    l << value
                }
            } else if (!l.find { it == value }) {
                l << value
            }

            value = l
        }

        if (!repeatable && value instanceof List && ((List) value).size() == 1) {
            value = value[0]
        }
        obj[key] = value
    }

    boolean isInstanceOf(Map entity, String baseType) {
        def type = entity['@type']
        if (type == null)
            return false
        List<String> types = type instanceof String ? [(String) type] : (List<String>) type
        return ld ? types.any { ld.isSubClassOf(it, baseType) }
                  : types.contains(baseType)
    }

}

@CompileStatic
abstract class BaseMarcFieldHandler extends ConversionPart {

    /**
     * Keep actual field tag intact on handlers having a specific "when"
     * condition. (See {@link MatchRule}).
     */
    String baseTag
    String tag
    Map tokenMaps
    String definesDomainEntityType
    String link
    Map computeLinks
    boolean repeatable = false
    String resourceType
    String groupId
    Map linkRepeated = null
    boolean onlySubsequentRepeated = false

    static final ConvertResult OK = new ConvertResult(true)
    static final ConvertResult FAIL = new ConvertResult(false)

    BaseMarcFieldHandler(MarcRuleSet ruleSet, String tag, Map fieldDfn,
            String baseTag = tag) {
        this.ruleSet = ruleSet
        this.tag = tag
        this.baseTag = baseTag
        this.tokenMaps = ruleSet.conversion.tokenMaps
        if (fieldDfn.aboutType) {
            definesDomainEntityType = fieldDfn.aboutType
        }
        aboutEntityName = fieldDfn.aboutEntity ?: '?thing'

        repeatable = fieldDfn.containsKey('addLink')
        link = linkTerm(fieldDfn.link ?: fieldDfn.addLink, repeatable)
        resourceType = typeTerm(fieldDfn.resourceType)
        groupId = fieldDfn.groupId
        embedded = fieldDfn.embedded == true

        Map dfn = (Map) fieldDfn['linkEveryIfRepeated']
        if (fieldDfn['linkSubsequentRepeated']) {
            assert !dfn, "linkEveryIfRepeated and linkSubsequentRepeated not allowed on ${fieldId}"
            dfn = (Map) fieldDfn['linkSubsequentRepeated']
            onlySubsequentRepeated = true
        }
        if (dfn) {
            def dfnRepeatable = dfn.containsKey('addLink')
            linkRepeated = [
                    link        : linkTerm(dfn.addLink ?: dfn.link, dfnRepeatable),
                    repeatable  : dfnRepeatable,
                    resourceType: typeTerm(dfn.resourceType),
                    groupId     : dfn.groupId,
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
        if (this.linkRepeated) {
            Map linkRepeated = [:]
            linkRepeated.putAll(this.linkRepeated)
            Collection fieldGroup = (Collection) state.sourceMap[baseTag]
            if (fieldGroup.size() > 1) {
                boolean firstOccurrence = value.is(fieldGroup[0][baseTag])
                if (!(firstOccurrence && onlySubsequentRepeated)) {
                    if (linkRepeated.groupId) {
                        linkRepeated.groupId = makeGroupId(
                                baseTag, (String) linkRepeated.groupId,
                                fieldGroup, value, -1)
                    }
                    return linkRepeated
                }
            }
        }
        String groupId = this.groupId ?
            makeGroupId(baseTag, this.groupId,
                    (Collection) state.sourceMap[baseTag], value)
            : null
        return [
                link        : this.link,
                repeatable  : this.repeatable,
                resourceType: this.resourceType,
                groupId     : groupId,
                embedded    : this.embedded
        ]
    }

    /**
     * The groupId mechanism combines fields describing the same entity.
     *
     * This is based on a heuristic of "balanced" groups, where the fields in
     * question have to use the same groupId, and be repeated exactly the same
     * number of times.
     */
    String makeGroupId(String tag, String groupId, Collection fieldGroup, value,
            int offset = 0) {
        def seq = (fieldGroup.withIndex()).findResult { it, int i ->
            if (it[tag].is(value)) {
                return "g${fieldGroup.size() + offset}-${i + 1 + offset}"
            }
        }
        // TODO: also support $6/$8
        return groupId.replaceFirst('\\$seq', "$seq")
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
            if (!isColKey(key) || !obj)
                return
            def colNums = parseColumnNumbers(key)
            colNums.eachWithIndex { Tuple2<Integer, Integer> colNum, int i ->
                columns << new Column(ruleSet, obj, colNum.first, colNum.second,
                        obj['itemPos'] ?: colNums.size() > 1 ? i : null,
                        obj['fixedDefault'],
                        obj['matchAsDefault'])

                if (colNum.second > fieldSize) {
                    fieldSize = colNum.second
                }
            }
        }
        columns.sort { it.start }
    }

    static boolean isColKey(key) { ((String) key)?.startsWith('[') }

    static List<Tuple2<Integer, Integer>> parseColumnNumbers(key) {
        List colNums = []
        (key =~ /\[(\d+)(?::(\d+))?\]\s*/).each { List<String> m ->
            Integer start = m[1].toInteger()
            Integer end = m[2]?.toInteger() ?: start + 1
            colNums << new Tuple2<>(start, end)
        }
        return colNums
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
            String obj = (String) col.revert(data)
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
        Integer itemPos
        String fixedDefault
        Pattern matchAsDefault

        Column(ruleSet, fieldDfn, int start, int end,
               itemPos, fixedDefault, matchAsDefault = null) {
            super(ruleSet, null, fieldDfn)
            assert start > -1 && end >= start
            this.start = start
            this.end = end
            this.itemPos = (Integer) itemPos
            this.fixedDefault = fixedDefault
            if (fixedDefault) {
                assert this.fixedDefault.size() == this.width
            }
            if (matchAsDefault) {
                this.matchAsDefault = Pattern.compile((String) matchAsDefault)
            }
            if (!fixedDefault && (!tokenMap ||
                    (!tokenMap.containsKey(FIXED_FAUX_NONE) &&
                    !tokenMap.containsKey(FIXED_NONE)))) {
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

        @CompileStatic(SKIP)
        def revert(Map data) {
            def v = super.revert(data, null)
            if ((v == null || v.every { it == null }) && fixedDefault)
                return fixedDefault

            if (v instanceof List) {
                v = v.findAll { it && width >= it.size() }
                if (itemPos != null) {
                    return itemPos < v.size() ? v[itemPos] : fixedDefault
                } else {
                    return v[0]
                }
            }
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
            def newEnt = newEntity(state, linkRule.resourceType, linkRule.groupId, linkRule.embedded)
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
    static final String COLUMN_STRING_PROPERTY = 'code'

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
            // NOTE: This is shared with repeated link in BaseMarcFieldHandler.
            // We thus disallow a combination of addLink and addProperty to
            // work around that. Could be improved...
            assert !fieldDfn.containsKey('addLink')
            repeatable = true
        }
        property = propTerm(fieldDfn.property ?: fieldDfn.addProperty,
                fieldDfn.containsKey('addProperty'))

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
                ent[COLUMN_STRING_PROPERTY] = value
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
                if (uriTemplate) {
                    def token = findTokenFromId(it)
                    if (token) {
                        return revertObject(token)
                    }
                    if (it instanceof Map) {
                        for (same in Util.asList(it['sameAs'])) {
                            token = findTokenFromId(same)
                            if (token) {
                                return revertObject(token)
                            }
                        }
                    }
                }
            }
        }
    }

    String findTokenFromId(node) {
        def id = node instanceof Map ? node['@id'] : node
        if (id instanceof String) {
            String token = extractToken(uriTemplate, (String) id)
            if (token != null && (!matchUriToken || matchUriToken.matcher(token).matches())) {
                return token
            }
        }
        return null
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
    List<List<MarcSubFieldHandler>> orderedAndGroupedSubfields
    List<MatchRule> matchRules
    Map<String, Map> pendingResources
    String aboutAlias
    List<String> onRevertPrefer
    Set<String> sharesGroupIdWith = new HashSet<String>()

    static GENERIC_REL_URI_TEMPLATE = "generic:{_}"

    @CompileStatic(SKIP)
    MarcFieldHandler(MarcRuleSet ruleSet, String tag, Map fieldDfn,
            String baseTag = tag) {
        super(ruleSet, tag, fieldDfn, baseTag)
        ind1 = fieldDfn.i1 ? new MarcSubFieldHandler(this, "ind1", fieldDfn.i1) : null
        ind2 = fieldDfn.i2 ? new MarcSubFieldHandler(this, "ind2", fieldDfn.i2) : null
        pendingResources = fieldDfn.pendingResources
        pendingResources?.values().each {
            linkTerm(it.link ?: it.addLink, it.containsKey('addLink'))
            typeTerm(it.resourceType)
            propTerm(it.property ?: it.addProperty, it.containsKey('addProperty'))
        }

        aboutAlias = fieldDfn.aboutAlias

        dependsOn = fieldDfn.dependsOn

        constructProperties = fieldDfn.constructProperties

        if (fieldDfn.uriTemplate) {
            uriTemplate = fieldDfn.uriTemplate
            uriTemplateKeys = fromTemplate(uriTemplate).variables as Set
            uriTemplateDefaults = fieldDfn.uriTemplateDefaults
        }
        onRevertPrefer = (List<String>) (fieldDfn.onRevertPrefer instanceof String ?
                [fieldDfn.onRevertPrefer] : fieldDfn.onRevertPrefer)

        computeLinks = (fieldDfn.computeLinks) ? new HashMap(fieldDfn.computeLinks) : [:]
        if (computeLinks) {
            computeLinks.use = computeLinks.use.replaceFirst(/^\$/, '')
        }

        matchRules = MatchRule.parseRules(this, fieldDfn) ?: Collections.emptyList()

        fieldDfn.each { key, obj ->
            def m = key =~ /^\$(\w+)$/
            if (m && obj) {
                addSubfield(m.group(1), obj)
            }
        }

        completePunctuationRules(subfields.values())

        orderedAndGroupedSubfields = orderAndGroupSubfields(subfields, (String) fieldDfn.subfieldOrder)

        assert !resourceType || link || computeLinks != null, "Expected link on $fieldId with resourceType: $resourceType"
        assert !embedded || link || computeLinks != null, "Expected link on embedded $fieldId"
    }

    void addSubfield(String code, Map dfn) {
        def subHandler = subfields[code] = new MarcSubFieldHandler(this, code, dfn)
        def aboutId = subHandler.about
        def checkAllPending = true
        if (checkAllPending && aboutId && aboutId != aboutAlias) {
            assert aboutId in pendingResources, "Missing pendingResources in ${fieldId}, cannot use ${aboutId} for ${subHandler.code}"
        }
    }

    @CompileStatic(SKIP)
    static List<List<MarcSubFieldHandler>> orderAndGroupSubfields(Map<String, MarcSubFieldHandler> subfields, String subfieldOrderRepr) {
        Map order = [:]
        if (subfieldOrderRepr) {
            subfieldOrderRepr.split(/\s+/).eachWithIndex { code, i ->
                order[code] = i
            }
        }
        if (!order['...']) {
            order['...'] = order.size()
        }
        Closure getOrder = {
            [order.get(it.code, order['...']), !it.code.isNumber(), it.code]
        }
        // Only the first subfield in a group is used to determine the order of the group
        return subfields.values().groupBy {
            it.about ?: it.fieldHandler.aboutAlias ?: it.code
        }.entrySet().sort {
            getOrder(it.value[0])
        }.collect {
            it.value.sort(getOrder)
        }
    }

    @CompileStatic(SKIP)
    static void completePunctuationRules(Collection subfields) {
        def charSet = new HashSet<Character>()
        // This adds all possible leading to be stripped from all other subfields.
        // IMPROVE: just add seen leading to all subfields preceding current?
        // (Pro: might remove chars too aggressively. Con: might miss some
        // punctuation in weirdly ordered subfields.)
        subfields.each {
            def lead = it.leadingPunctuation
            if (lead) charSet.addAll(lead.toCharArray())
        }
        subfields.each {
            it.punctuationChars = (((it.punctuationChars ?: []) as Set) + charSet) as char[]
        }
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

        def localEntities = [:]

        if (aboutAlias) {
            localEntities[aboutAlias] = entity
        }

        [ind1: ind1, ind2: ind2].each { indKey, handler ->
            if (!handler)
                return
            def ok = handler.convertSubValue(state, value, value[indKey], entity,
                    uriTemplateParams, localEntities)
            if (!ok && handler.marcDefault == null) {
                unhandled << indKey
            }
        }

        String precedingCode = null
        boolean precedingNewAbout = false
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
                        ok = subDfn.convertSubValue(state, value, subVal, ent,
                                uriTemplateParams, localEntities,
                                code != precedingCode && precedingNewAbout)
                    }
                    precedingNewAbout = subDfn.newAbout
                    precedingCode = code
                }
                if (!ok && !handled.contains(code)) {
                    unhandled << code
                }
            }
        }

        if (constructProperties) {
            constructProperties.each { key, dfn ->
                if (!(key in entity)) {
                    def parts = Util.getAllByPath(entity, (String) dfn.property)
                    if (parts?.size() > 1 && !parts.any { it.is(null) }) {
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

        // If absorbSingle && only one item: merge it with parent.
        localEntities.keySet().each {
            if (it == aboutAlias) return
            def pending = (Map) pendingResources[it]
            if (pending.absorbSingle) {
                def link = (String) (pending.link ?: pending.addLink)
                def parent = (Map) (pending.about ? localEntities[pending.about] : entity)
                def items = (List<Map>) parent[link]
                if (items instanceof List && items.size() == 1) {
                    parent.remove(link)
                    parent.putAll(items[0])
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

            newEnt = newEntity(state, linkRule.resourceType, linkRule.groupId, linkRule.embedded)

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

    Map getLocalEntity(Map state, Map fieldValue, Map owner, String pendingKey, Map localEntities, boolean forceNew = false) {
        Map entity = (Map) localEntities[pendingKey]
        if (entity == null || forceNew) {
            def pending = pendingResources[pendingKey]

            String pendingId = pending.groupId ?
                makeGroupId(baseTag, (String) pending.groupId,
                        (Collection) state.sourceMap[baseTag], fieldValue)
                : null

            entity = localEntities[pendingKey] = newEntity(state,
                    (String) pending.resourceType,
                    pendingId,
                    (Boolean) pending.embedded)
            def link = (String) (pending.link ?: pending.addLink)
            if (pending.about) {
                owner = getLocalEntity(state, fieldValue, owner, (String) pending.about, localEntities)
            }
            addValue(owner, link, entity, pending.containsKey('addLink'))
        }
        return entity
    }

    @CompileStatic(SKIP)
    def revert(Map data, Map result, List<MatchRule> usedMatchRules = []) {

        def matchedResults = []

        // NOTE: Each possible match rule might produce data from *different* entities.
        // If this overproduces, it's because _revertedBy fails to prevent it.
        for (rule in matchRules) {
            def matchres = rule.handler.revert(data, result, usedMatchRules + [rule])
            if (matchres) {
                matchedResults += matchres
            }
        }

        final Map topEntity = getEntity(data)

        if (definesDomainEntityType && !isInstanceOf(topEntity, definesDomainEntityType)) {
            return null
        }

        def results = []

        def useLinks = []
        if (computeLinks && computeLinks.mapping instanceof Map) {
            computeLinks.mapping.each { code, compLink ->
                if (compLink in topEntity) {
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

        String uriTemplateBase = uriTemplate
        if (uriTemplate) {
            int braceIdx = uriTemplate.indexOf('{')
            if (braceIdx > -1) {
                uriTemplateBase = uriTemplate.substring(0, braceIdx)
            }
        }
        for (useLink in useLinks) {
            def useEntities = [topEntity]
            if (useLink.link) {
                useEntities = Util.asList(topEntity[useLink.link])
                if (useLink.resourceType) {
                    useEntities = useEntities.findAll {
                        if (!it) return false
                        assert it instanceof Map, "Error reverting ${fieldId} - expected object, got: ${it}"
                        if (uriTemplateBase) {
                            if (!it['@id'] || !it['@id'].startsWith(uriTemplateBase)) {
                                return false
                            }
                        }
                        return isInstanceOf(it, useLink.resourceType)
                    }
                }
            }

            useEntities.each {
                def (boolean requiredOk, Map aboutMap) = buildAboutMap(aboutAlias, pendingResources, it)
                if (requiredOk) {
                    def field = revertOne(data, topEntity, it, aboutMap, usedMatchRules)
                    if (field) {
                        if (useLink.subfield) {
                            field.subfields << useLink.subfield
                        }
                        results << field
                    }
                }
            }
        }

        return results + matchedResults
    }

    @CompileStatic(SKIP)
    Tuple2<Boolean, Map<String, List>> buildAboutMap(String aboutAlias, Map pendingResources, Map entity) {
        Map<String, List> aboutMap = [:]
        boolean requiredOk = true

        if (aboutAlias) {
            aboutMap[aboutAlias] = [entity]
        }

        if (pendingResources) {
            // TODO: fatigue HACK; instead collect recursively...
            for (int i = pendingResources.size(); i > 0; i--) {
                pendingResources?.each { key, pending ->
                    if (key in aboutMap) {
                        return
                    }
                    def resourceType = pending.resourceType
                    def parents = pending.about == null ? [entity] :
                        pending.about in aboutMap ? aboutMap[pending.about] : null

                    parents?.each {
                        def about = it[pending.link ?: pending.addLink]
                        if (!about && pending.absorbSingle) {
                            if (isInstanceOf(it, pending.resourceType)) {
                                about = it
                            } else {
                                requiredOk = false
                            }
                        }
                        Util.asList(about).eachWithIndex { item, pos ->
                            if (!item || (pending.resourceType && !isInstanceOf(item, pending.resourceType))) {
                                return
                            } else if (pos == 0 && pending.itemPos == 'rest') {
                                return
                            } else if (pos > 0 && pending.itemPos == 'first') {
                                return
                            }
                            aboutMap.get(key, []).add(item)
                        }
                    }
                }
            }
        }
        pendingResources?.each { key, pending ->
            if (pending.required && !(key in aboutMap)) {
                requiredOk = false
            }
        }

        return new Tuple2<Boolean, Map>(requiredOk, aboutMap)
    }

    @CompileStatic(SKIP)
    def revertOne(Map data, Map topEntity, Map currentEntity, Map<String, List> aboutMap = null,
                    List<MatchRule> usedMatchRules) {

        MatchRule usedMatchRule = usedMatchRules ? usedMatchRules?.last() : null

        def i1Entities = ind1?.about ? aboutMap[ind1.about] : [currentEntity]
        def i2Entities = ind2?.about ? aboutMap[ind2.about] : [currentEntity]

        def i1 = usedMatchRules.findResult { it.ind1 } ?: ind1 ? i1Entities.findResult { ind1.revert(data, it) } : ' '
        def i2 = usedMatchRules.findResult { it.ind2 } ?: ind2 ? i2Entities.findResult { ind2.revert(data, it) } : ' '

        def subs = []
        def failedRequired = false

        def usedEntities = new HashSet()

        def prevAdded = null

        orderedAndGroupedSubfields.each { subhandlers ->

            def about = subhandlers[0].about ?: aboutAlias

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

                // TODO: This check is rather crude for determining if an
                // entity has already been reverted. There *might* be several
                // fields contributing to a nested entity. (Using groupId for
                // those is now adviced.)
                if (
                        (selectedEntity != topEntity &&
                         selectedEntity._revertedBy && (!groupId || selectedEntity._groupId != groupId)) ||
                        selectedEntity._revertedBy == baseTag ||
                        selectedEntity._revertedBy in sharesGroupIdWith) {
                    failedRequired = true
                    return
                    //subs << ['DEBUG:blockedSinceRevertedBy': selectedEntity._revertedBy]
                }

                def value = subhandler.revert(data, selectedEntity)

                def justAdded = null

                if (value instanceof List) {
                    value.each { v ->
                        if (!usedMatchRules || usedMatchRules.every { it.matchValue(code, v) }) {
                            def sub = [(code): v]
                            subs << sub
                            justAdded = [code, sub]
                        }
                    }
                    if (subhandler.required && !justAdded) {
                        failedRequired = true
                    }
                } else if (value != null) {
                    if (!usedMatchRules || usedMatchRules.every { it.matchValue(code, value) }) {
                        def sub = [(code): value]
                        subs << sub
                        justAdded = [code, sub]
                    }
                } else {
                    if (subhandler.required || subhandler.requiresI1 || subhandler.requiresI2) {
                        failedRequired = true
                    }
                }
                if (!failedRequired) {
                    usedEntities << selectedEntity
                    if (prevAdded && justAdded &&  subhandler.leadingPunctuation) {
                        def (prevCode, prevSub) = prevAdded
                        def prevValue = prevSub[prevCode]
                        def nextLeading = subhandler.leadingPunctuation
                        if (!nextLeading.startsWith(' ')) {
                            nextLeading = ' ' + nextLeading
                        }
                        prevSub[prevCode] = prevValue + nextLeading
                    }
                }
                if (justAdded) prevAdded = justAdded
            }
        }

        if (!failedRequired && i1 != null && i2 != null && subs.size()) {
            def field = [ind1: i1, ind2: i2, subfields: subs]

            if (usedMatchRules && !usedMatchRules.every { it.matches(field) }) {
                //return [_notMatching: this.tag]
                return null
            }

            // TODO: store reverted input refs instead of tagging input data
            usedEntities.each { it._revertedBy = baseTag; it._groupId = groupId }
            //field._revertedBy = this.tag
            return field
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
    String trailingPunctuation
    String leadingPunctuation
    String link
    String about
    boolean newAbout
    boolean altNew
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
        trailingPunctuation = subDfn.trailingPunctuation
        leadingPunctuation = subDfn.leadingPunctuation
        punctuationChars = (subDfn.punctuationChars ?: trailingPunctuation?.trim())?.toCharArray()
        surroundingChars = subDfn.surroundingChars?.toCharArray()
        super.setTokenMap(fieldHandler, subDfn)

        if (subDfn.aboutNew) {
            about = subDfn.aboutNew
            newAbout = true
        } else if (subDfn.aboutAltNew) {
            about = subDfn.aboutAltNew
            newAbout = true
            altNew = true
        } else {
            about = subDfn.about
        }

        repeatable = subDfn.containsKey('addLink')
        link = linkTerm(subDfn.link ?: subDfn.addLink, repeatable)

        repeatProperty = subDfn.containsKey('addProperty')
        property = propTerm(subDfn.property ?: subDfn.addProperty, repeatProperty)

        resourceType = typeTerm(subDfn.resourceType)

        required = subDfn.required == true
        overwrite = subDfn.overwrite == true

        if (subDfn.uriTemplate) {
            subUriTemplate = subDfn.uriTemplate
        }

        if (subDfn.splitValuePattern) {
            /*TODO: assert subDfn.splitValuePattern=~ /^\^.+\$$/,
                   'For explicit safety, these patterns must start with ^ and end with $' */
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

    boolean convertSubValue(Map state, Map value, def subVal, Map ent,
                            Map uriTemplateParams, Map localEntities,
                            boolean precedingNewAbout = false) {
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
            // A new local entity is created if the subfield triggers a new
            // entity ("about"), unless it's an alternative trigger and a new
            // entity was just triggered (i.e. the preceding subfield has a
            // different code and also triggers a new entity).
            ent = fieldHandler.getLocalEntity(state, value, ent, about, localEntities,
                    altNew && precedingNewAbout ? false : newAbout)
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
            if (marcDefault == null || subVal != marcDefault) {
                if (overwrite) {
                    ent[property] = subVal
                } else {
                    fieldHandler.addValue(ent, property, subVal, repeatProperty)
                }
                fieldHandler.addValue(uriTemplateParams, uriTemplateKeyBase + property, subVal, true)
            }
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
            return marcDefault ?: null
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

            if (resourceType && !isInstanceOf(entity, resourceType))
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

            if (value && trailingPunctuation) {
                value += trailingPunctuation
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
        return matchDefs?.collect {
            Map dfnCopy = fieldDfn.findAll { it.key != 'match' }
            if (dfnCopy.pendingResources) {
                dfnCopy.pendingResources = [:] + (Map) dfnCopy.pendingResources
            }
            MarcRuleSet.mergeFieldDefinitions((Map) it, dfnCopy, parent.tag, false)
            new MatchRule(parent, dfnCopy, ruleWhenMap)
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
        handler = new MarcFieldHandler(parent.ruleSet, tag, dfn, parent.baseTag)
        // Fixate matched indicator values in nested match rules
        handler.matchRules.each {
            if (ind1) {
                if (it.ind1) assert ind1 == it.ind1
                it.ind1 = ind1
            }
            if (ind2) {
                if (it.ind2) assert ind2 == it.ind2
                it.ind2 = ind2
            }
        }
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
            // TODO: !isInstanceOf(entity, matchDomain)
            if (entity['@type'] != matchDomain)
                return null
            else if (whenTests.size() == 0)
                return handler
        }
        return  matches(value) ? handler : null
    }

    boolean matches(value) {
        return whenAll ? whenTests.every { it(value) } : whenTests.any { it(value) }
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
                if (value instanceof List) {
                    results.addAll(value)
                } else {
                    results << value
                }
            }
        }
    }

}

class MalformedFieldValueException extends RuntimeException {}

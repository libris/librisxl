package whelk.converter.marc

import groovy.transform.CompileStatic
import groovy.transform.Immutable
import groovy.transform.NullCheck
import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.JsonLd
import whelk.component.DocumentNormalizer
import whelk.converter.FormatConverter
import whelk.filter.LanguageLinker
import whelk.filter.LinkFinder

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.Temporal
import java.util.regex.Pattern

import static com.damnhandy.uri.template.UriTemplate.fromTemplate
import static groovy.transform.TypeCheckingMode.SKIP

@Log
class MarcFrameConverter implements FormatConverter {

    ObjectMapper mapper = new ObjectMapper()
    LinkFinder linkFinder
    JsonLd ld
    RomanizationStep.LanguageResources languageResources
    
    String configResourceBase = "ext"
    String marcframeFile = "marcframe.json"

    protected MarcConversion conversion
    
    MarcFrameConverter(LinkFinder linkFinder = null, JsonLd ld = null, RomanizationStep.LanguageResources languageResources = null) {
        this.linkFinder = linkFinder
        this.languageResources = languageResources
        setLd(ld)
    }

    void setLd(JsonLd ld) {
        this.ld = ld
        if (ld) initialize()
    }

    void initialize() {
        if (conversion) return
        initialize((Map) readConfig(marcframeFile))
    }

    void initialize(Map config) {
        Map tokenMaps = config.tokenMaps
        conversion = new MarcConversion(this, config, tokenMaps)
    }

    Object readConfig(String path) {
        return getClass().classLoader.getResourceAsStream("$configResourceBase/$path").withStream {
            Object config = mapper.readValue(it, Object)
            expandIncludes(config)
            return config
        }
    }

    void expandIncludes(o) {
        if (o instanceof List) {
            o.each { expandIncludes(it) }
        } else if (o instanceof Map) {
            if ('@include' in o) {
                def included = (Map) readConfig(o['@include'])
                o.clear()
                o.putAll(included)
            }
            o.each {
                if (it.value instanceof Map && '@include' in it.value) {
                    it.value = readConfig(it.value['@include'])
                } else {
                    expandIncludes(it.value)
                }
            }
        }
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
        } else if (data[JsonLd.RECORD_KEY]) {
            // Reshape a framed top-level thing with a meta record
            Map thing = data.clone()
            Map record = thing.remove(JsonLd.RECORD_KEY)
            record.mainEntity = thing
            data = record
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
    Map defaultPunctuation

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

        defaultPunctuation = config.defaultPunctuation ?: [:]

        MARC_CATEGORIES.each { marcCat ->
            def marcRuleSet = new MarcRuleSet(this, marcCat)
            marcRuleSets[marcCat] = marcRuleSet
            marcRuleSet.buildHandlers(config)
        }
        addTypeMaps()

        missingTerms.each {
            log.debug("Missing: $it.term a $it.type")
        }
        badRepeats.each {
            if (it.term.startsWith(converter.ld.expand('marc:')))
                return
            if (it.shouldRepeat)
                log.debug("Expected to be repeatable: $it.term")
            else
                log.debug("Unexpected repeat of: $it.term")
        }
    }

    void checkTerm(String key, String type, boolean repeatable) {
        if (!key || key == '@type' || !converter.ld)
            return
        def terms = converter.ld.vocabIndex.keySet()
        String term = key
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
        def procStep
        switch (stepDfn.type) {
            case 'RestructPropertyValuesAndFlag':
            procStep = new RestructPropertyValuesAndFlagStep(props); break
            case 'MappedProperty':
            procStep = new MappedPropertyStep(props); break
            case 'VerboseRevertData':
            procStep = new VerboseRevertDataStep(props); break
            case 'ProduceIfMissing':
            procStep = new ProduceIfMissingStep(props); break
            case 'SetFlagsByPatterns':
            procStep = new SetFlagsByPatternsStep(props); break
            case 'CopyOnRevert':
            procStep = new CopyOnRevertStep(props); break
            case 'InjectWhenMatchingOnRevert':
            procStep = new InjectWhenMatchingOnRevertStep(props); break
            case 'Romanization':
            procStep = new RomanizationStep(props)
            procStep.converter = converter    
            procStep.languageResources = converter.languageResources; break
            case null:
            return null
            default:
            throw new RuntimeException("Unknown postProcStep: ${stepDfn}")
        }
        procStep.ld = converter.ld
        procStep.mapper = converter.mapper
        procStep.init()
        return procStep
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

        if (doPostProcessing) {
            sharedPostProcSteps.each {
                it.modify(record, thing)
            }
            marcRuleSet.postProcSteps.each {
                it.modify(record, thing)
            }
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
            applyInverses(data, data[marcRuleSet.thingLink])
            sharedPostProcSteps.each {
                it.unmodify(data, data[marcRuleSet.thingLink])
            }
            marcRuleSet.postProcSteps.each {
                it.unmodify(data, data[marcRuleSet.thingLink])
            }
        }

        Map state = [:]
        Map marc = [:]
        List fields = []
        marc['fields'] = fields
        marcRuleSet.revertFieldOrder.each { tag ->
            def handler = marcRuleSet.fieldHandlers[tag]
            def value = handler.revert(state, data, marc)
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

        if (data._marcUncompleted) {
            List<Map> marcUncompleted = data._marcUncompleted instanceof List
                ? data._marcUncompleted
                : [data._marcUncompleted]

            marcUncompleted.each {
                // Only re-add *fully* uncompleted fields. (Saved failed fields
                // with subfields in unhandled are now excluded to avoid field
                // duplication).
                if (!it.containsKey('_unhandled')) {
                    fields << it.clone()
                }
            }
        }

        return marc
    }

    void applyInverses(Map record, Map thing) {
        converter.ld?.applyInverses(thing)
    }
}

class MarcRuleSet {

    static PREPROC_TAGS = ["000", "001", "006", "007", "008"] as Set

    MarcConversion conversion
    String name

    // NOTE: use of these is rather rigid. (Also see buildAboutMap.)
    String thingLink
    String definingTrait

    def fieldHandlers = [:]
    List<MarcFramePostProcStep> postProcSteps

    Set primaryTags = new HashSet()

    Iterable<String> revertFieldOrder = new LinkedHashSet<>()

    // aboutTypeMap is used on revert to determine which ruleSet to use
    Map<String, Set<String>> aboutTypeMap = new HashMap<String, Set<String>>()

    Map<String, Map> topPendingResources = [:]
    List<String> topPendingKeys

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
            } else if (tag.startsWith('TODO:')) {
                return
            }

            if (dfn.size() == 0) {
                return
            }

            thingLink = topPendingResources['?thing'].link
            definingTrait = topPendingResources['?work']?.link

            dfn = processInherit(config, subConf, tag, dfn)

            dfn = processInclude(config, dfn, tag)

            if (dfn.aboutType && dfn.aboutType != JsonLd.RECORD_TYPE) {
                if (dfn.aboutEntity) {
                    assert tag && aboutTypeMap.containsKey(dfn.aboutEntity)
                }
                aboutTypeMap[dfn.aboutEntity ?: '?thing'] << dfn.aboutType
            }
            for (matchDfn in dfn['match']) {
                if (matchDfn.aboutType && matchDfn.aboutType != JsonLd.RECORD_TYPE) {
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
                if (!handler.ignored) {
                    assert handler.property || handler.uriTemplate,
                            "Incomplete: $tag: $dfn"
            }
            }

            fieldHandlers[tag] = handler
        }

        topPendingKeys = Util.getSortedPendingKeys(topPendingResources)

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
        // IMPROVE:
        // - definingTrait now has to be one of instanceOf or itemOf or none.
        // - Improvable now: use ld.isSubClassOf(thing[TYPE], thingType)
        //   (and remove this crude type-check fallback).
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
                def uri = dfn.uriTemplate == ConversionPart.UNESCAPED_URI_SLOT ?
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

            boolean uriTokensOk = true
            dfn.matchUriTokens?.each { recordKey, matchUriToken ->
                if (!(record[recordKey] =~ matchUriToken)) {
                    uriTokensOk = false
                }
            }

            if (dfn.uriTemplate && uriTokensOk) {
                try {
                    builtEntityId = conversion.resolve(
                            fromTemplate(dfn.uriTemplate)
                                    .set('marcType', name).set(record).expandPartial())
                } catch (IllegalArgumentException e) {
                    // Fails on resolve if expanded is only partially filled
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

    static final String URI_SLOT = '{_}'
    static final String UNESCAPED_URI_SLOT = '{+_}'

    MarcRuleSet ruleSet
    String aboutEntityName
    String fallbackEntityName
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
            if (dfn.reverseTokenMapOverrides) {
                def revMap = dfn.reverseTokenMapOverrides
                if (revMap instanceof String) {
                    revMap = fieldHandler.tokenMaps[revMap]
                }
                if (revMap instanceof Map) {
                    reverseTokenMap.putAll(revMap)
                }
            }
        }
    }

    Map getEntity(Map state, Map data, String aboutAlias = '?record') {
        boolean dataIsRecord = ruleSet.thingLink in data
        if (!dataIsRecord) {
            return data
        }

        Map<String, List> aboutMap
        if (!state.aboutMap) {
            Tuple2 okAndMap = buildAboutMap(ruleSet.topPendingResources, ruleSet.topPendingKeys, data, aboutAlias)
            assert okAndMap[0]
            aboutMap = (Map<String, List>) okAndMap[1]
            state.aboutMap = aboutMap
        } else {
            aboutMap = (Map<String, List>) state.aboutMap
        }

        Map aboutEntity = (Map) aboutMap[aboutEntityName]?.get(0) ?: data
        if (fallbackEntityName) {
            Map fallbackEntity = (Map) aboutMap[fallbackEntityName]?.get(0)
            if (fallbackEntity)
                return deepMergedClone(fallbackEntity, aboutEntity)
        }
        return aboutEntity
    }

    Map deepMergedClone(Map keepSome, Map keepAll) {
        def result = [:]
        Set keySet = keepAll.keySet() + keepSome.keySet()
        for (Object key : keySet) {

            Object highPrioValue = keepAll[key]
            Object lowPrioValue = keepSome[key]

            if (!highPrioValue && lowPrioValue)
                result.put(key, lowPrioValue)
            else if (highPrioValue && !lowPrioValue)
                result.put(key, highPrioValue)
            else if (highPrioValue && lowPrioValue) {
                if (highPrioValue instanceof Map && lowPrioValue instanceof Map) {
                    result.put(key, deepMergedClone((Map)lowPrioValue, (Map)highPrioValue))
                }
                else if (highPrioValue instanceof List && lowPrioValue instanceof List) {
                    List resultingList = []
                    resultingList.addAll( (List) highPrioValue )
                    resultingList.addAll( (List) lowPrioValue )
                    result.put(key, resultingList)
                }
            }
        }

        return result
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
            value = ((List) value)[0]
        }
        obj[key] = value
    }

    boolean isInstanceOf(Map entity, String baseType) {
        return ld ? ld.isInstanceOf(entity, baseType) : Util.asList(entity['@type']).contains(baseType)
    }

    Tuple2<Boolean, Map<String, List>> buildAboutMap(Map pendingResources, List<String> pendingKeys, Map entity, String aboutAlias) {
        Map<String, List> aboutMap = [:]
        boolean requiredOk = true

        if (aboutAlias) {
            aboutMap[aboutAlias] = [entity]
        }

        if (pendingResources) {
            pendingKeys.each { String key ->
                if (key in aboutMap) {
                    return
                }
                Map pendingDfn = pendingResources[key] as Map
                def resourceType = pendingDfn.resourceType
                def parents = pendingDfn.about == null ? [entity] :
                    pendingDfn.about in aboutMap ? aboutMap[pendingDfn.about] : null

                parents?.each { parent ->
                    String link = pendingDfn.link ?: pendingDfn.addLink
                    def about = parent[link]
                    if (!about && pendingDfn.infer == true) {
                        about = ld.getSubProperties(link).findResult { parent[it] }
                    }

                    if (!about && pendingDfn.absorbSingle) {
                        if (isInstanceOf(parent as Map, pendingDfn.resourceType as String)) {
                            about = parent
                        } else {
                            requiredOk = false
                        }
                    }
                    Util.asList(about).eachWithIndex { item, pos ->
                        if (!item || (pendingDfn.resourceType && !isInstanceOf(item as Map, pendingDfn.resourceType as String))) {
                            return
                        } else if (pos == 0 && pendingDfn.itemPos == 'rest') {
                            return
                        } else if (pos > 0 && pendingDfn.itemPos == 'first') {
                            return
                        }
                        aboutMap.get(key, []).add(item)
                    }
                    def typeCandidates = pendingDfn.allowedTypesOnRevert
                    if (typeCandidates) {
                        def items = aboutMap.get(key)
                        def selected = []
                        for (type in typeCandidates) {
                            // NOTE: using isInstanceOf would be preferable
                            // over direct type comparison, but won't work
                            // since a base type might be preferable to a
                            // subtype in practise (due to a modeling issue).
                            selected = items?.findAll { it['@type'] == type  }
                            if (selected) {
                                items = selected
                                break
                            }
                        }
                        aboutMap[key] = selected
                    }
                }
            }
        }
        // TODO: we can now move this into the loop, yes?
        boolean missingRequired = pendingResources?.find { key, pendingDfn ->
            pendingDfn['required'] && !(key in aboutMap)
        }
        if (missingRequired) {
            requiredOk = false
        }

        // The about map is the whole embellished record N times, framed around the "pending keys", typically:
        // ?record, ?thing, ?work, _:provision
        return new Tuple2<Boolean, Map<String, List>>(requiredOk, aboutMap)
    }

    static String findTokenFromId(Object node, String uriTemplate,
            Pattern matchUriToken) {
        def id = node instanceof Map ? node['@id'] : node
        if (!(id instanceof String)) {
            return null
        }

        String token = extractToken(uriTemplate, URI_SLOT, (String) id)
        if (token != null) {
            token = URLDecoder.decode(token)
        } else {
            token = extractToken(uriTemplate, UNESCAPED_URI_SLOT, (String) id)
        }

        if (token == null) {
            return null
        }
        if (matchUriToken && !matchUriToken.matcher(token).matches()) {
            return null
        }
        return token
    }

    static String extractToken(String tplt, String slot, String link) {
        if (!link) {
            return null
        }
        def i = tplt.indexOf(slot)
        if (i == -1) {
            return
        }
        def before = tplt.substring(0, i)
        def after = tplt.substring(i + URI_SLOT.size())
        if (link.startsWith(before) && link.endsWith(after)) {
            def part = link.substring(before.size())
            return part.substring(0, part.size() - after.size())
        }
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
    boolean ignored = false
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
        fallbackEntityName = fieldDfn.fallbackEntity

        repeatable = fieldDfn.containsKey('addLink')
        link = linkTerm(fieldDfn.link ?: fieldDfn.addLink, repeatable)
        resourceType = typeTerm(fieldDfn.resourceType)
        ignored = fieldDfn.ignored == true
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

    abstract def revert(Map state, Map data, Map result)

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
                columns << new Column(this, obj as Map, colNum.v1, colNum.v2,
                        obj['itemPos'] ?: colNums.size() > 1 ? i : null,
                        obj['fixedDefault'],
                        obj['ignoreOnRevert'],
                        obj['matchAsDefault'])

                if (colNum.v2 > fieldSize) {
                    fieldSize = colNum.v2
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
            def convertable = col.convert(state, value)
            if (convertable && !convertable.ok) {
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
    def revert(Map state, Map data, Map result, boolean keepEmpty = false) {
        def value = new StringBuilder(FIXED_NONE * fieldSize)
        def actualValue = false
        for (col in columns) {
            assert value.size() > col.start // columns must fit within value
            String obj = (String) col.revert(state, data)
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
        Boolean ignoreOnRevert
        Pattern matchAsDefault
        MarcFixedFieldHandler fixedFieldHandler

        Column(MarcFixedFieldHandler fixedFieldHandler, Map fieldDfn, int start, int end,
               itemPos, fixedDefault, ignoreOnRevert = null, matchAsDefault = null) {
            super(fixedFieldHandler.ruleSet, "$fixedFieldHandler.tag-$start-$end", fieldDfn)
            this.fixedFieldHandler = fixedFieldHandler
            assert start > -1 && end >= start
            this.start = start
            this.end = end
            this.itemPos = (Integer) itemPos
            this.fixedDefault = fixedDefault
            this.ignoreOnRevert = ignoreOnRevert

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
            return super.convert(state, token)
        }

        @CompileStatic(SKIP)
        def revert(Map state, Map data) {
            def v = super.revert(state, data, null)
            if (ignoreOnRevert && fixedDefault)
                return fixedDefault

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
            typeName = fieldDfn.baseTypeMap?.get(typeName) ?: typeName
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

    def revert(Map state, Map data, Map result) {
        // NOTE: using rootEntity instead of data here fails on revert of bib 008
        def entities = [data]
        def rootEntity = getEntity(state, data)
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
                value = baseConverter.revert(state, entity, result, true)
            def tokenBasedConverter = handlerMap[getToken(result.leader, value)]
            if (tokenBasedConverter) {
                def overlay = tokenBasedConverter.revert(state, entity, result, true)
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

    static final String COLUMN_STRING_PROPERTY = 'code'
    static final String UNDEF_VALUE = "|"
    static final String NONE_VALUE = " "

    String property
    String uriTemplate
    Pattern matchUriToken = null
    boolean parseZeroPaddedNumber
    DateTimeFormatter dateTimeFormat
    ZoneId timeZone
    LocalTime defaultTime
    boolean missingCentury = false
    Boolean silentRevert = null
    // TODO: working, but not so useful until capable of merging entities..
    //MarcSimpleFieldHandler linkedHandler

    @CompileStatic(SKIP)
    MarcSimpleFieldHandler(MarcRuleSet ruleSet, String tag, Map fieldDfn) {
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

        String parseDateTime = fieldDfn.parseDateTime
        if (parseDateTime) {
            missingCentury = (parseDateTime == "yyMMdd")
            if (missingCentury) {
                parseDateTime = "yy$parseDateTime".toString()
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
        uriTemplate = fieldDfn.uriTemplate
        if (fieldDfn.matchUriToken) {
            matchUriToken = Pattern.compile(fieldDfn.matchUriToken as String)
            if (fieldDfn.matchSpec) {
                fieldDfn.matchSpec['matches'].each {
                    assert matchUriToken.matcher(it).matches()
                }
                fieldDfn.matchSpec['notMatches'].each {
                    assert !matchUriToken.matcher(it).matches()
                }
            }
        }
        silentRevert = fieldDfn.silentRevert
        //if (fieldDfn.linkedEntity) {
        //    linkedHandler = new MarcSimpleFieldHandler(ruleSet,
        //            tag + ":linked", fieldDfn.linkedEntity)
        //}
    }

    ConvertResult convert(Map state, value) {
        if (ignored) {
            return OK
        }
        if (!(property || link)) {
            return null
        }

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

        boolean isNothing = value.find {
            it != NONE_VALUE && it != UNDEF_VALUE
        } == null

        if (isNothing)
            return OK

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
                // pass
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
                // FIXME: Throw away? Or sameAs "broken link"?
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
    def revert(Map state, Map data, Map result) {
        def entity = getEntity(state, data)
        if (link) {
            entity = entity[link]
        }
        if (entity && property) {
            def v = entity[property]
            if (v) {
                if (dateTimeFormat) {
                    try {
                        if (v instanceof List)
                            v = v[0]
                        if (!(v instanceof String))
                            return null
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
                    String token = findTokenFromId(it, uriTemplate, matchUriToken)
                    if (token) {
                        return revertToken(it, token)
                    }
                    if (it instanceof Map) {
                        // TODO: def enumCandidates = relations.findDependers(id, MATCHING_LINKS) // TODO: of sought after enum type? Won't reduce selection as things are imolemented right now...
                        for (same in Util.asList(it['sameAs'])) {
                            token = findTokenFromId(same, uriTemplate, matchUriToken)
                            if (token) {
                                return revertToken(it, token)
                            }
                        }
                    }
                }
            }
        }
    }

    protected def revertToken(Map object, String token) {
        def v = revertObject(token)
        if (v && silentRevert == false) {
            object._revertedBy = baseTag
        }
        return v
    }

    static ZonedDateTime parseDate(String s) {
        try {
            return ZonedDateTime.parse(s, DT_FORMAT)
        } catch (DateTimeParseException e) {
            return ZonedDateTime.parse(s, DT_FORMAT_FALLBACK)
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
    List<String> pendingKeys
    String aboutAlias
    //NOTE: allowLinkOnRevert as list may be preferable, but there is currently no case for it. Update if needed.
    List<String> allowLinkOnRevert
    List<String> onRevertPrefer
    Set<String> sharesGroupIdWith = new HashSet<String>()
    boolean silentRevert

    static GENERIC_REL_URI_TEMPLATE = "generic:{_}"

    MarcFieldHandler(MarcRuleSet ruleSet, String tag, Map fieldDfn,
            String baseTag = tag) {
        super(ruleSet, tag, fieldDfn, baseTag)
        ind1 = fieldDfn['i1'] ? new MarcSubFieldHandler(this, "ind1", fieldDfn.i1 as Map) : null
        ind2 = fieldDfn['i2'] ? new MarcSubFieldHandler(this, "ind2", fieldDfn.i2 as Map) : null
        pendingResources = fieldDfn['pendingResources'] as Map<String, Map>
        pendingResources?.values()?.each {
            linkTerm(it.link ?: it.addLink, it.containsKey('addLink'))
            typeTerm(it.resourceType)
            propTerm(it.property ?: it.addProperty, it.containsKey('addProperty'))
        }
        if (pendingResources) {
            pendingKeys = Util.getSortedPendingKeys(pendingResources)
        }
        
        aboutAlias = fieldDfn['aboutAlias']
        allowLinkOnRevert = Util.asList(fieldDfn['allowLinkOnRevert'])
        dependsOn = fieldDfn['dependsOn'] as List<String>
        constructProperties = fieldDfn['constructProperties'] as Map<String, Map>

        if (fieldDfn['uriTemplate']) {
            uriTemplate = fieldDfn['uriTemplate']
            uriTemplateKeys = fromTemplate(uriTemplate).variables as Set
            uriTemplateDefaults = fieldDfn['uriTemplateDefaults'] as Map
        }
        onRevertPrefer = (List<String>) (fieldDfn.onRevertPrefer instanceof String ?
                [fieldDfn.onRevertPrefer] : fieldDfn.onRevertPrefer)

        silentRevert = fieldDfn.silentRevert == true

        computeLinks = (fieldDfn.computeLinks) ? new HashMap(fieldDfn['computeLinks'] as Map) : [:]
        if (computeLinks) {
            computeLinks.use = ((String) computeLinks['use']).replaceFirst(/^\$/, '')
        }

        matchRules = MatchRule.parseRules(this, fieldDfn) ?: Collections.emptyList() as List<MatchRule>

        fieldDfn.each { key, obj ->
            def m = key =~ /^\$(\w+)$/
            if (m && obj) {
                addSubfield(m.group(1) as String, obj as Map)
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
        
        if (!order['6']) {
            order['6'] = -1 // MARC 21, Appendix A: "Subfield $6 is always the first subfield in the field."
        }
        
        Closure getOrder = {
            [order.get(it.code, order['...']), !it.code.isNumber(), it.code]
        }
        // Only the highest subfield of a group is used to determine the order of the group.
        Set<String> aboutGroups = subfields.values().findResults {
            it.newAbout ? it.about : null
        }
        return subfields.values().groupBy {
            it.about in aboutGroups ? it.about : it.code
        }.entrySet().sort {
            getOrder(it.value.sort(getOrder)[0])
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
        if (ignored) {
            return OK
        }
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
            if (rule.onRevert != null)
                continue
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

        Map<String, Map> localEntities = [:]

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
                if (subDfn?.ignored) {
                    return
                }
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

        shallowRemoveEmptyNodes(aboutEntity)

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
                def computedUri = fromTemplate(uriTemplate).expand(uriTemplateParams as Map<String, Object>)
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
        localEntities.each { String localKey, Map localEntity ->
            if (localKey == aboutAlias) return
            def pending = (Map) pendingResources[localKey]

            if (pending.uriTemplate && !localEntity.containsKey('@id')) {
                def uri = fromTemplate((String) pending.uriTemplate).expand((Map) localEntity)
                if (uri) {
                    localEntity['@id'] = uri
                }
            }

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

    /**
     * Remove empty nodes and nodes having just a type. Only does a shallow
     * sweep in values of the given entity.
     */
    void shallowRemoveEmptyNodes(Map entity) {
        Iterator entryIt = entity.entrySet().iterator()
        Closure isEmpty = {
            it instanceof Map && (it.size() == 0 ||
                    (it.size() == 1 &&
                     it.containsKey('@type')))
        }
        entryIt.each {
            if (it.value instanceof List) {
                ((List)it.value).removeAll(isEmpty)
                if (((List)it.value).size() == 0) {
                    entryIt.remove()
                }
            } else if (it.value instanceof Map) {
                if (isEmpty(it.value)) {
                    entryIt.remove()
                }
            }
        }
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
    def revert(Map state, Map data, Map result, List<MatchRule> usedMatchRules = []) {

        def matchedResults = []

        // NOTE: Each possible match rule might produce data from *different* entities.
        // If this overproduces, it's because _revertedBy fails to prevent it.
        for (rule in matchRules) {
            def matchres = rule.handler.revert(state, data, result, usedMatchRules + [rule])
            if (rule.handler.ignored && matchres) {
                return null
            }

            if (matchres) {
                matchedResults += matchres
            }
        }

        final Map topEntity = getEntity(state, data)

        if (!matchedResults && definesDomainEntityType && !isInstanceOf(topEntity, definesDomainEntityType)) {
            return null
        }

        def results = []

        def useLinks = []

        allowLinkOnRevert.each {
            useLinks << [link: it, resourceType: resourceType]
        }

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
            if (!onlySubsequentRepeated && linkRepeated.link in topEntity) {
                useLinks = [linkRepeated]
            } else {
                useLinks << linkRepeated
            }
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
                                boolean aliasMatchesBase = Util.asList(it['sameAs']).any {
                                    it.get('@id')?.startsWith(uriTemplateBase)
                                }
                                if (!aliasMatchesBase) {
                                    return false
                                }
                            }
                        }
                        return isInstanceOf(it, useLink.resourceType)
                    }
                }
            }

            useEntities.each {
                def (boolean requiredOk, Map aboutMap) = buildAboutMap(pendingResources, pendingKeys, it, aboutAlias)
                if (requiredOk) {
                    def field = revertOne(state, data, topEntity, it, aboutMap, usedMatchRules, useLink.groupId ?: this.groupId)
                    if (field) {
                        if (useLink.subfield) {
                            field.subfields.add(0, useLink.subfield)
                        }
                        results << field
                    }
                }
            }
        }

        return results + matchedResults
    }

    @CompileStatic(SKIP)
    def revertOne(Map state, Map data, Map topEntity, Map currentEntity, Map<String, List> aboutMap = null,
                    List<MatchRule> usedMatchRules, String groupId) {
        if (usedMatchRules.any { !it.acceptRevert(currentEntity) }) {
            return null
        }

        String i1 = usedMatchRules.findResult { it.ind1 } ?: revertIndicator(ind1, state, data, currentEntity, aboutMap)
        String i2 = usedMatchRules.findResult { it.ind2 } ?: revertIndicator(ind2, state, data, currentEntity, aboutMap)

        def subs = []
        def failedRequired = false
        def onlySupplementary = true

        def usedEntities = new HashSet()

        def prevAdded = null

        // NOTE: Within a field, only *one* positioned term is supported.
        Map firstRelPosSubfield = null
        Map sortedByItemPos = [:]

        Set requiredCodes = new HashSet()
        Set succeededCodes = new HashSet()

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

                if (subhandler.required) {
                    requiredCodes << code
                }

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
                    return
                    //subs << ['DEBUG:blockedSinceRevertedBy': selectedEntity._revertedBy]
                }

                List result = subhandler.revertWithSourcePosition(state, data, selectedEntity)

                def justAdded = null
                String firstAddedValue = null

                if (result != null) {
                    result.each {
                        def (vs, pos) = it
                        if (!(vs instanceof List)) {
                            vs = [vs]
                        }
                        for (v in vs.flatten()) {
                            if (usedMatchRules?.any { !it.matchValue(code, v) }) {
                                continue
                            }
                            boolean repeatOk = (subhandler.link && subhandler.repeatable) ||
                                               (subhandler.property && subhandler.repeatProperty)
                            if (justAdded && !repeatOk) {
                                break
                            }

                            Map sub = [(code): v]

                            if (subhandler.itemPos == 'rest') {
                                if (firstRelPosSubfield == null) {
                                    firstRelPosSubfield = sub
                                }
                                sortedByItemPos[System.identityHashCode(sub)] = pos
                            }

                            subs << sub
                            justAdded = [code, sub]
                            if (firstAddedValue == null) {
                                firstAddedValue = v
                            }
                        }
                    }
                    if (subhandler.required && !justAdded
                        && !(code in succeededCodes)) {
                        failedRequired = true
                    }
                } else {
                    if ((subhandler.required || subhandler.requiresI1 ||
                                subhandler.requiresI2)
                        && !(code in succeededCodes)) {
                        failedRequired = true
                    }
                }
                if (!failedRequired && justAdded) {
                    usedEntities << selectedEntity
                    succeededCodes << code

                    if (prevAdded && justAdded && subhandler.leadingPunctuation) {
                        def (prevCode, prevSub) = prevAdded
                        MarcSubFieldHandler prevSubHandler = subfields[prevCode]
                        if (!prevSubHandler.trailingPunctuation) {
                            def prevValue = prevSub[prevCode]
                            def nextLeading = subhandler.leadingPunctuation
                            boolean valueSkipsLeading = subhandler.skipLeadingPattern &&
                                    subhandler.skipLeadingPattern.matcher(firstAddedValue).matches()
                            boolean prevValueBlocksLeading = prevSubHandler.blockNextLeadingPattern
                                                                .matcher(prevValue).matches()
                            if (!valueSkipsLeading && !prevValueBlocksLeading &&
                                !prevValue.endsWith(nextLeading)) {
                                prevSub[prevCode] = prevValue + nextLeading
                            }
                        }
                    }
                }
                if (justAdded) {
                    if (subhandler.supplementary != true) {
                        onlySupplementary = false
                    }
                    prevAdded = justAdded
                }
            }
        }

        if (requiredCodes && !requiredCodes.every { it in succeededCodes }) {
            failedRequired = true
        }

        if (!failedRequired && i1 != null && i2 != null && subs.size() && !onlySupplementary) {
            if (sortedByItemPos.size()) {
                int relPosStart = subs.indexOf(firstRelPosSubfield)
                subs.sort {
                    def relPos = sortedByItemPos[System.identityHashCode(it)]
                    [relPos != null ? relPosStart : subs.indexOf(it), relPos]
                }
            }
            def field = [ind1: i1, ind2: i2, subfields: subs]

            if (usedMatchRules && !usedMatchRules.every { it.matches(field) }) {
                //return [_notMatching: this.tag]
                return null
            }

            // TODO: store reverted input refs instead of tagging input data
            usedEntities.each {
                def revertMark = silentRevert ? '_silentlyRevertedBy' : '_revertedBy'
                it[revertMark] = baseTag
                it._groupId = groupId
            }
            //field._revertedBy = this.tag
            return field
        } else {
            return null
        }

    }

    private String revertIndicator(MarcSubFieldHandler indHandler,
            Map state, Map data, Map currentEntity,
            Map<String, List> aboutMap) {
        def entities = indHandler?.about ? aboutMap[indHandler.about] : [currentEntity]
        if (!indHandler) {
            return ' '
        }
        def v = entities?.findResult { indHandler.revert(state, data, (Map) it) }
        return (String) (v instanceof List ? v.get(0) : v)
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
    Pattern skipLeadingPattern
    Pattern blockNextLeadingPattern
    boolean balanceBrackets
    String link
    String about
    boolean newAbout
    boolean altNew
    boolean repeatable
    String property
    boolean repeatProperty
    boolean overwrite
    boolean infer
    String resourceType
    String subUriTemplate
    Pattern matchUriToken = null
    Pattern splitValuePattern
    List<String> splitValueProperties
    Pattern castPattern
    String castProperty
    String rejoin
    String joinEnd
    boolean allowEmpty
    String definedElsewhereToken
    String marcDefault
    boolean ignored = false
    boolean ignoreOnRevert = false
    String onRevertAppendValueFrom
    boolean required = false
    boolean supplementary = false
    String requiresI1
    String requiresI2
    // TODO: itemPos is not used right now. Only supports first/rest.
    String itemPos

    @CompileStatic(SKIP)
    MarcSubFieldHandler(MarcFieldHandler fieldHandler, code, Map subDfn) {
        this.ruleSet = fieldHandler.ruleSet
        this.fieldHandler = fieldHandler
        this.code = code
        super.setTokenMap(fieldHandler, subDfn)
        aboutEntityName = subDfn.aboutEntity
        fallbackEntityName = subDfn.fallbackEntity
        ignored = subDfn.ignored == true
        ignoreOnRevert = subDfn.ignoreOnRevert == true

        trailingPunctuation = subDfn.trailingPunctuation
        leadingPunctuation = subDfn.leadingPunctuation

        punctuationChars = (subDfn.containsKey('punctuationChars')
                            ? subDfn.punctuationChars
                            : (trailingPunctuation ?:
                               defaultPunctuation['punctuationChars']))?.toCharArray()

        surroundingChars = subDfn.surroundingChars?.toCharArray()

        skipLeadingPattern = toPattern(subDfn.containsKey('skipLeading')
                                       ? subDfn.skipLeading
                                       : defaultPunctuation['skipLeading'])

        blockNextLeadingPattern = toPattern(subDfn.containsKey('blockNextLeading')
                                            ? subDfn.blockNextLeading
                                            : defaultPunctuation['blockNextLeading'])

        balanceBrackets = (subDfn.containsKey('balanceBrackets')
                            ? subDfn.balanceBrackets
                            : defaultPunctuation['balanceBrackets']) == true

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

        if (subDfn.uriTemplate) {
            subUriTemplate = subDfn.uriTemplate
        }
        if (subDfn.matchUriToken) {
            matchUriToken = Pattern.compile(subDfn.matchUriToken)
        }

        repeatProperty = subDfn.containsKey('addProperty')
        property = propTerm(subDfn.property ?: subDfn.addProperty, repeatProperty)

        resourceType = typeTerm(subDfn.resourceType)

        required = subDfn.required == true
        supplementary = subDfn.supplementary == true
        assert !required || !supplementary

        overwrite = subDfn.overwrite == true

        infer = subDfn.infer == true

        if (subDfn.splitValueProperties) {
            /*TODO: assert subDfn.splitValuePattern=~ /^\^.+\$$/,
                   'For explicit safety, these patterns must start with ^ and end with $' */
            // TODO: support repeatable?
            if (subDfn.splitValuePattern)
                splitValuePattern = Pattern.compile(subDfn.splitValuePattern)
            splitValueProperties = subDfn.splitValueProperties
            rejoin = subDfn.rejoin
            joinEnd = subDfn.joinEnd
            allowEmpty = subDfn.allowEmpty
        }
        if (subDfn.castPattern) {
            castPattern = Pattern.compile(subDfn.castPattern)
            castProperty = subDfn.castProperty
        }
        marcDefault = subDfn.marcDefault
        definedElsewhereToken = subDfn.definedElsewhereToken
        onRevertAppendValueFrom = subDfn.onRevertAppendValueFrom
        requiresI1 = subDfn['requires-i1']
        requiresI2 = subDfn['requires-i2']
        itemPos = subDfn.itemPos

        assert !resourceType || link, "Expected link on ${fieldHandler.fieldId}-$code"
    }

    Map getDefaultPunctuation() {
        ruleSet.conversion.defaultPunctuation
    }

    Pattern toPattern(String pattern) {
        pattern ? Pattern.compile(pattern) : null
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

            if (subUriTemplate &&
                (matchUriToken == null ||
                 matchUriToken.matcher((String) subVal).matches())) {
                try {
                    entId = subUriTemplate == UNESCAPED_URI_SLOT ?
                        subVal : fromTemplate(subUriTemplate).expand(["_": subVal])
                } catch (IllegalArgumentException|IndexOutOfBoundsException e) {
                    // Bad characters in what should have been a proper URI path ('+' expansion).
                     // NOTE: We just drop the attempt here if the uriTemplate fails...
                }
            }
            def newEnt = newEntity(state, resourceType, entId)
            addValue(ent, link, newEnt, repeatable)
            ent = newEnt
            uriTemplateKeyBase = "${link}."
            ok = true
        }

        def useProperty = this.property
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
        if (!didSplit && castPattern) {
            def m = castPattern.matcher((String) subVal)
            if (m) {
                useProperty = castProperty
            }
        }
        if (!didSplit && useProperty) {
            if (marcDefault == null || subVal != marcDefault) {
                if (overwrite) {
                    ent[useProperty] = subVal
                } else {
                    fieldHandler.addValue(ent, useProperty, subVal, repeatProperty)
                }
                fieldHandler.addValue(uriTemplateParams, uriTemplateKeyBase + useProperty, subVal, true)
            }
            ok = true
        }

        return ok
    }

    String clearChars(String val) {
        if (val.size() > 1) {
            val = val.trim()
            if (punctuationChars) {
                for (c in punctuationChars) {
                    if (val.size() < 2) {
                        break
                    }
                    if (val[-1] == c.toString()) {
                        val = val[0..-2].trim()
                    }
                }
            }
            if (surroundingChars) {
                for (c in surroundingChars) {
                    if (val.size() < 2) {
                        break
                    }
                    if (val[-1] == c.toString()) {
                        val = val[0..-2].trim()
                    } else if (val[0] == c.toString()) {
                        val = val[1..-1].trim()
                    }
                }
            }
        }

        // Rudimentary mending of broken '[...]' expressions:
        if (balanceBrackets) {
            val = toBalancedBrackets(val)
        }

        return val
    }

    static String toBalancedBrackets(String val) {
        if (val.startsWith('[')) {
            if (val.indexOf(']') == -1) {
                val += ']'
            }
        } else if (val.endsWith(']') &&
                    val.indexOf('[') == -1) {
            val = '[' + val
        }
        return val
    }

    @CompileStatic(SKIP)
    List revert(Map state, Map data, Map currentEntity) {
        List result = revertWithSourcePosition(state, data, currentEntity)
        if (result == null)
            return null
        return result.collect { it[0] }
    }

    @CompileStatic(SKIP)
    List revertWithSourcePosition(Map state, Map data, Map currentEntity) {
        currentEntity = aboutEntityName ? getEntity(state, data) : currentEntity
        if (currentEntity == null)
            return null

        def entities = link ? currentEntity[link] : [currentEntity]
        if (entities == null) {
            return marcDefault ? [marcDefault] : null
        }
        if (entities instanceof Map) {
            entities = [entities]
        }

        List values = []

        int i = 0
        for (entity in entities) {
            i++
            if (i == 1) {
                if (itemPos == "rest")
                    continue
            } else if (itemPos == "first") {
                break
            }

            String entityId = entity['@id']

            def propertyValue = ld ? ld.getPropertyValue(entity, property) : entity[property]

            if (ignoreOnRevert) {
                continue
            }

            // TODO: Later, it may be desirable to add functionality
            // to specify the punctuation mark to be used when merging.
            if (onRevertAppendValueFrom) {
                if (entity.containsKey(onRevertAppendValueFrom)) {
                    String valueToAppend = entity[onRevertAppendValueFrom] instanceof List ?
                            entity[onRevertAppendValueFrom].join(" ") : entity[onRevertAppendValueFrom]
                    String mergedVal = "${entity[property]} ${valueToAppend}"
                    propertyValue = mergedVal
                }
            }

            if (propertyValue == null && castProperty)
                propertyValue = entity[castProperty]

            if (propertyValue == null && infer) {
                for (subProp in ld.getSubProperties(property)) {
                    propertyValue = ld.getPropertyValue(entity, subProp)
                    if (propertyValue)
                        break
                }
            }

            boolean checkResourceType = true

            boolean extractPropertyFromLinkIfMissing =
                    property && !propertyValue && !entity['@type'] && subUriTemplate

            if (extractPropertyFromLinkIfMissing) {
                propertyValue = findTokenFromId(entityId, subUriTemplate, matchUriToken)
                if (propertyValue) {
                    checkResourceType = false
                }
            }

            if (checkResourceType && resourceType &&
                    !isInstanceOf(entity, resourceType)) {
                continue
            }

            if (splitValueProperties && rejoin &&
                (!propertyValue || property in splitValueProperties)) {
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
                        if (v instanceof List) {
                            v = v.join(', ')
                        }
                        vs << v
                    }
                }
                if (vs.size() == splitValueProperties.size() && !allEmpty) {
                    def value = vs.join(rejoin)
                    if (joinEnd && !value.endsWith(joinEnd)) {
                        value += joinEnd
                    }
                    values << [value, i]
                    continue
                }
            }

            def value = null
            if (property) {
                value = revertObject(propertyValue)
            } else if (link) {
                def obj = entityId
                if (obj && subUriTemplate) {
                    def token = findTokenFromId(obj, subUriTemplate, matchUriToken)
                    if (token) {
                        obj = token
                    }
                }
                value = revertObject(obj)
            }

            if (value instanceof String && value.size() > 0) {
                if (surroundingChars) {
                    def (lead, tail) = surroundingChars
                    if (value[0] != lead) {
                        value = '' + lead + value
                    }
                    if (value[-1] != tail) {
                        value += tail
                    }
                }
                if (trailingPunctuation &&
                        !value.endsWith(trailingPunctuation)) {
                    value += trailingPunctuation
                }
            }

            if (value != null) {
                values << [value, i]
            } else if (marcDefault) {
                values << [marcDefault, i]
            }
        }

        if (values.size() == 0)
            return null
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
        } as Map<String, Map>
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
    Map onRevert = null
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
        def when = dfn.remove('when')
        if (when instanceof String) {
            parseWhen(parent.fieldId, (String) when)
        } else if (when instanceof Map) {
            onRevert = (Map) when['onRevert']
            if (onRevert) {
                Map then = (Map) when['then']
                if (then) {
                    if (then.ind1) ind1 = then.ind1
                    if (then.ind2) ind2 = then.ind2
                }
            }
        }
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
        if (onRevert) {
            // If this passes acceptRevert, we're good.
            return true
        }
        return whenAll ? whenTests.every { it(value) } : whenTests.any { it(value) }
    }

    boolean acceptRevert(Map entity) {
        if (onRevert == null) {
            return true
        } else {
            objectContains(entity, onRevert)
        }
    }

    @CompileStatic(SKIP)
    static boolean objectContains(obj, pattern) {
        if (pattern instanceof List) {
            return pattern.every {
                objectContains(obj, it)
            }
        }
        if (pattern instanceof Map) {
            if (obj instanceof List) {
                return obj.any {
                    objectContains(it, pattern)
                }
            }
            if (!(obj instanceof Map)) {
                return false
            }
            def map = (Map) obj
            for (Map.Entry entry : ((Map) pattern).entrySet()) {
                if (!map.containsKey(entry.key)) {
                    return false
                }
                return objectContains(map[entry.key], entry.value)
            }
            return true
        } else {
            return obj == pattern
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
                if (value instanceof List) {
                    results.addAll(value)
                } else {
                    results << value
                }
            }
        }
    }

    static List<String> getSortedPendingKeys(Map<String, Map> pendingResources) {
        Map<String, List> pendingDeps = getPendingDeps(pendingResources)
        return pendingResources.keySet().sort().sort { pendingDeps[it].size() }
    }

    static Map<String, List> getPendingDeps(Map<String, Map> pendingResources) {
        Map<String, List> pendingDeps = [:]
        pendingResources.each { String key, Map pending ->
            def deps = pendingDeps.get(key, [])
            String about = pending.about
            while (about) {
                assert about != key
                deps << about
                about = pendingResources.get(about)?.about
            }
        }
        return pendingDeps
    }

}

class MalformedFieldValueException extends RuntimeException {}

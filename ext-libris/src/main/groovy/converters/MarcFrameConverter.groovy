package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import java.util.regex.Pattern
import org.codehaus.jackson.map.ObjectMapper

import se.kb.libris.whelks.Document

import se.kb.libris.conch.converter.MarcJSONConverter

import com.damnhandy.uri.template.UriTemplate


@Log
class MarcFrameConverter extends BasicFormatConverter {

    URIMinter uriMinter
    ObjectMapper mapper = new ObjectMapper()

    protected MarcConversion conversion

    MarcFrameConverter(uriSpacePath="oldspace.json") {
        def loader = getClass().classLoader

        loader.getResourceAsStream(uriSpacePath).withStream {
            uriMinter = new LibrisURIMinter(mapper.readValue(it, Map))
        }

        def config = loader.getResourceAsStream("marcframe.json").withStream {
            mapper.readValue(it, Map)
        }
        def tokenMaps = [:]
        config.tokenMaps.each { key, sourceRef ->
            if (sourceRef instanceof String) {
                tokenMaps[key] = loader.getResourceAsStream(sourceRef).withStream {
                    mapper.readValue(it, List).collectEntries { [it.code, it] }
                }
            } else {
                tokenMaps[key] = sourceRef
            }
        }
        conversion = new MarcConversion(config, uriMinter, tokenMaps)
    }

    Map createFrame(Map marcSource) {
        return conversion.createFrame(marcSource)
    }

    @Override
    String getResultContentType() { "application/ld+json" }

    @Override
    String getRequiredContentType() { "application/x-marc-json" }

    Document doConvert(final Object record, final Map metaentry) {
        def source = MarcJSONConverter.toJSONMap(record)
        def result = createFrame(source)
        log.trace("Created frame: $result")

        return new Document().withData(mapper.writeValueAsBytes(result)).setMetaEntry(metaentry).withContentType(getResultContentType())
    }

    @Override
    Document doConvert(final Document doc) {
        def source = doc.dataAsMap
        def result = createFrame(source)
        log.trace("Created frame: $result")

        return new Document().withIdentifier(doc.identifier).withData(mapper.writeValueAsBytes(result)).withEntry(doc.entry).withMeta(doc.meta).withContentType("application/ld+json")
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
            result = converter.conversion.revert(source)
        } else {
            result = converter.createFrame(source)
        }
        converter.mapper.writeValue(System.out, result)
    }

}


class MarcConversion {

    static PREPROC_TAGS = ["000", "001", "006", "007", "008"] as Set

    Map marcTypeMap = [:]
    def marcHandlers = [:]
    Map tokenMaps
    Set primaryTags = new HashSet()

    URIMinter uriMinter

    MarcConversion(Map config, URIMinter uriMinter, Map tokenMaps) {
        marcTypeMap = config.marcTypeFromTypeOfRecord
        this.uriMinter = uriMinter
        this.tokenMaps = tokenMaps
        ['bib', 'auth', 'hold'].each {
            buildHandlers(config, it)
        }
    }

    String getMarcCategory(leader) {
        def typeOfRecord = getTypeOfRecord(leader)
        return marcTypeMap[typeOfRecord] ?: marcTypeMap['*']
    }

    String getTypeOfRecord(leader) {
        return leader.substring(6, 7)
    }

    String getBibLevel(leader) {
        return leader.substring(7, 8)
    }

    void buildHandlers(config, marcCategory) {
        def fieldHandlers = marcHandlers[marcCategory] = [:]
        def subConf = config[marcCategory]
        subConf.each { tag, fieldDfn ->
            if (fieldDfn.inherit) {
                fieldDfn = processInherit(config, subConf, tag, fieldDfn)
            }
            if (fieldDfn.ignored || fieldDfn.size() == 0) {
                return
            }
            def handler = null
            if (fieldDfn.tokenTypeMap) {
                handler = new TokenSwitchFieldHandler(this, tag, fieldDfn)
            } else if (fieldDfn.recTypeBibLevelMap) {
                handler = new TokenSwitchFieldHandler(this, tag, fieldDfn, 'recTypeBibLevelMap')
            } else if (fieldDfn.find { it.key[0] == '[' }) {
                handler = new MarcFixedFieldHandler(this, tag, fieldDfn)
            } else if (fieldDfn.find { it.key[0] == '$' }) {
                handler = new MarcFieldHandler(this, tag, fieldDfn)
                if (handler.dependsOn) {
                    primaryTags += handler.dependsOn
                }
                if (handler.definesDomainEntityType != null) {
                    primaryTags << tag
                }
            }
            else {
                handler = new MarcSimpleFieldHandler(this, tag, fieldDfn)
                assert handler.property || handler.uriTemplate, "Incomplete: $tag: $fieldDfn"
            }
            fieldHandlers[tag] = handler
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

    Map createFrame(Map marcSource, String recordId=null) {

        def record = ["@type": "Record", "@id": recordId]

        def marcRemains = [failedFixedFields: [:], uncompleted: [], broken: []]

        def entityMap = [Record: record, marcRemains: marcRemains]
        // TODO:
        // * always one record and a primary "thing"
        // * the type of this thing is determined during processing
        def work = [:]
        def instance = [:]
        entityMap['Instance'] = instance
        entityMap['Work'] = work
        record.about = instance

        def leader = marcSource.leader
        def marcCat = getMarcCategory(leader)
        def fieldHandlers = marcHandlers[marcCat]

        def preprocFields = []
        def primaryFields = []
        def otherFields = []
        def sourceMap = [leader: leader]

        marcSource.fields.each { field ->
            field.each { tag, value ->
                def fieldsByTag = sourceMap[tag]
                if (fieldsByTag == null) {
                    fieldsByTag = sourceMap[tag] = []
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

        fieldHandlers["000"].convert(sourceMap, leader, entityMap)

        processFields(fieldHandlers, sourceMap, preprocFields, entityMap)

        processFields(fieldHandlers, sourceMap, primaryFields, entityMap)
        processFields(fieldHandlers, sourceMap, otherFields, entityMap)

        if (marcRemains.uncompleted.size() > 0) {
            record._marcUncompleted = marcRemains.uncompleted
        }
        if (marcRemains.broken.size() > 0) {
            record._marcBroken = marcRemains.broken
        }
        if (marcRemains.failedFixedFields.size() > 0) {
            record._marcFailedFixedFields = marcRemains.failedFixedFields
        }

        // TODO: make this configurable
        if (record['@id'] == null) {
            def uriMap = uriMinter.computePaths(record, marcCat)
            record['@id'] = uriMap['document']
            instance['@id'] = uriMap['thing']
        }

        // TODO: only(?) bib (monographies), and use a config-defined link..
        if (work.find { k, v -> k != "@type" }) {
            instance['instanceOf'] = work
        }
        return record
    }

    void processFields(fieldHandlers, sourceMap, fields, entityMap) {
        fields.each { field ->
            try {
                def ok = false
                field.each { tag, value ->
                    def handler = fieldHandlers[tag]
                    if (handler) {
                        ok = handler.convert(sourceMap, value, entityMap)
                    }
                }
                if (!ok) {
                    entityMap.marcRemains.uncompleted << field
                }
            } catch (MalformedFieldValueException e) {
                entityMap.marcRemains.broken << field
            }
        }
    }

    Map revert(data) {
        def marc = [:]
        def fields = []
        marc['fields'] = fields
        def recType = "bib" // TODO: compute from about-type
        def fieldHandlers = marcHandlers[recType]
        fieldHandlers.each { tag, handler ->
            def value = handler.revert(data)
            if (tag == "000") {
                marc.leader = value
            } else {
                if (value == null)
                    return
                if (value instanceof List) {
                    value.each {
                        fields << [(tag): it]
                    }
                } else {
                    fields << [(tag): value]
                }
            }
        }
        return marc
    }

}

class TokenSwitchFieldHandler extends BaseMarcFieldHandler {

    MarcFixedFieldHandler baseConverter
    Map<String, MarcFixedFieldHandler> handlerMap = [:]
    boolean useRecTypeBibLevel = false
    String addLink = null
    String repeatedAddLink = null
    Map tokenNames = [:]

    TokenSwitchFieldHandler(conversion, tag, Map fieldDfn, tokenMapKey='tokenTypeMap') {
        super(conversion, tag)
        addLink = fieldDfn.addLink
        if (fieldDfn['match-repeated']) {
            repeatedAddLink = fieldDfn['match-repeated'].addLink
        }
        this.baseConverter = new MarcFixedFieldHandler(conversion, tag, fieldDfn)
        def tokenMap = fieldDfn[tokenMapKey]
        if (tokenMapKey == 'recTypeBibLevelMap') {
            this.useRecTypeBibLevel = true
            buildHandlersByRecTypeBibLevel(fieldDfn, tokenMap)
        } else {
            buildHandlersByTokens(fieldDfn, tokenMap)
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
        handlerMap[token] = new MarcFixedFieldHandler(conversion, tag, dfn)
    }

    String getToken(sourceMap, value, entityMap) {
        if (useRecTypeBibLevel) {
            def typeOfRecord = conversion.getTypeOfRecord(sourceMap.leader)
            def bibLevel = conversion.getBibLevel(sourceMap.leader)
            return typeOfRecord + bibLevel
        } else if (value) {
            return value[0]
        } else {
            return value
        }
    }

    boolean convert(sourceMap, value, entityMap) {
        def token = getToken(sourceMap, value, entityMap)
        def converter = handlerMap[token]
        if (converter == null)
            return false

        def addLink = this.addLink
        if (sourceMap[tag].size() > 1 && repeatedAddLink) {
            addLink = repeatedAddLink
        }
        if (addLink) {
            def ent = entityMap.Instance
            def newEnt = newEntity(null)
            addValue(ent, addLink, newEnt, true)
            entityMap = entityMap.clone()
            entityMap.Instance = newEnt
        }

        def baseOk = true
        if (baseConverter)
            baseOk = baseConverter.convert(sourceMap, value, entityMap)
        def ok = converter.convert(sourceMap, value, entityMap)
        return baseOk && ok
    }

    def revert(Map data) {
        def value = null
        if (baseConverter)
            value = baseConverter.revert(data)
        // TODO:
        //def converter = computeConverter(data)
        //return value combinedWith converter.convert(data, valueSb)
        return value
    }
}

class MarcFixedFieldHandler {

    String tag
    static final String FIXED_NONE = " "
    static final String FIXED_UNDEF = "|"
    def columns = []

    MarcFixedFieldHandler(conversion, tag, fieldDfn) {
        this.tag = tag
        fieldDfn.each { key, obj ->
            def m = (key =~ /^\[(\d+):(\d+)\]$/)
            if (m) {
                def start = m[0][1].toInteger()
                def end = m[0][2].toInteger()
                columns << new Column(conversion, obj, start, end, obj['default'])
            }
        }
        columns.sort { it.start }
    }

    boolean convert(sourceMap, value, entityMap) {
        def success = true
        def failedFixedFields = entityMap.marcRemains.failedFixedFields
        for (col in columns) {
            if (!col.convert(sourceMap, value, entityMap)) {
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
        return success
    }

    def revert(Map data) {
        def value = new StringBuilder(FIXED_NONE * (columns[-1].end))
        for (col in columns) {
            def obj = col.revert(data)
            // TODO: ambiguity trouble if this is a List!
            if (obj instanceof List) obj = obj.find { it }
            if (obj) {
                assert col.width - obj.size() > -1
                assert value.size() > col.start
                assert col.width >= obj.size()
                def end = col.start + obj.size() - 1
                value[col.start .. end] = obj
            }
        }
        return value.toString()
    }

    class Column extends MarcSimpleFieldHandler {
        int start
        int end
        String defaultValue
        Column(conversion, fieldDfn, start, end, defaultValue) {
            super(conversion, null, fieldDfn)
            assert start > -1 && end >= start
            this.start = start
            this.end = end
            this.defaultValue = defaultValue
        }
        int getWidth() { return end - start }
        String getToken(value) {
            if (value.size() < start)
                return ""
            if (value.size() < end)
                return value.substring(start)
            return value.substring(start, end)
        }
        boolean convert(sourceMap, value, entityMap) {
            def token = getToken(value)
            if (token == "")
                return true
            if (token == defaultValue)
                return true
            boolean isNothing = token.find { it != FIXED_NONE && it != FIXED_UNDEF } == null
            if (isNothing)
                return true
            return super.convert(sourceMap, token, entityMap)
        }
        def revert(Map data) {
            def v = super.revert(data)
            if (v == null && defaultValue)
                return defaultValue
            return v
        }
    }

}

class ConversionPart {

    String domainEntityName
    Map tokenMap
    Map reverseTokenMap

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
        if (domainEntityName == 'Record')
            return data
        if (domainEntityName == 'Work')
            return data.about.instanceOf
        else
            return data.about
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

}

abstract class BaseMarcFieldHandler extends ConversionPart {

    MarcConversion conversion
    String tag
    Map tokenMaps

    BaseMarcFieldHandler(conversion, tag) {
        this.conversion = conversion
        this.tag = tag
        this.tokenMaps = conversion.tokenMaps
    }

    abstract boolean convert(sourceMap, value, entityMap)

    abstract def revert(Map data)

    void addValue(obj, key, value, repeatable) {
        def current = obj[key]
        if (current || repeatable) {
            def l = current ?: []
            l << value
            value = l
        }
        obj[key] = value
    }

    Map newEntity(type, id=null) {
        def ent = [:]
        if (type) ent["@type"] = type
        if (id) ent["@id"] = id
        return ent
    }

}

class MarcSimpleFieldHandler extends BaseMarcFieldHandler {

    static final String DT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SZ"
    static final String URI_SLOT = '{_}'

    String property
    String link
    String rangeEntityName
    String uriTemplate
    Pattern matchUriToken = null
    boolean repeat = false
    String dateTimeFormat
    boolean ignored = false
    // TODO: working, but not so useful until capable of merging entities..
    //MarcSimpleFieldHandler linkedHandler

    MarcSimpleFieldHandler(conversion, tag, fieldDfn) {
        super(conversion, tag)
        super.setTokenMap(this, fieldDfn)
        if (fieldDfn.addProperty) {
            property = fieldDfn.addProperty
            repeat = true
        } else {
            property = fieldDfn.property
        }
        domainEntityName = fieldDfn.domainEntity ?: 'Instance'
        dateTimeFormat = fieldDfn.parseDateTime
        ignored = fieldDfn.get('ignored', false)
        if (fieldDfn.addLink) {
            link = fieldDfn.addLink
            repeat = true
        } else {
            link = fieldDfn.link
        }
        rangeEntityName = fieldDfn.rangeEntity
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
        //    linkedHandler = new MarcSimpleFieldHandler(conversion,
        //            tag + ":linked", fieldDfn.linkedEntity)
        //}
    }

    boolean convert(sourceMap, value, entityMap) {
        if (ignored || !(property || link))
            return

        if (tokenMap) {
            def mapped = tokenMap[value] ?: tokenMap[value.toLowerCase()]
            if (mapped == null) {
                return tokenMap.containsKey(value)
            } else if (mapped == false) {
                return true
            } else {
                value = mapped
            }
        }

        if (dateTimeFormat) {
            value = Date.parse(dateTimeFormat, value).format(DT_FORMAT)
        }

        def ent = entityMap[domainEntityName]
        if (ent == null)
            return false
        if (link) {
            def newEnt = newEntity(rangeEntityName)
            addValue(ent, link, newEnt, repeat)
            ent = newEnt
        }
        if (uriTemplate) {
            if (!matchUriToken || matchUriToken.matcher(value).matches()) {
                ent['@id'] = uriTemplate.replace(URI_SLOT, value.trim())
            } else {
                ent['@value'] = value
            }
        //} else if (linkedHandler) {
        //    linkedHandler.convert(sourceMap, value,[Instance: ent])
        } else {
            addValue(ent, property, value, repeat)
        }

        return true
    }

    def revert(Map data) {
        def entity = getEntity(data)
        if (link)
            entity = entity[link]
        if (property) {
            def v = entity[property]
            if (v && dateTimeFormat)
                return Date.parse(DT_FORMAT, v).format(dateTimeFormat)
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
    String definesDomainEntityType
    UriTemplate uriTemplate
    Map uriTemplateDefaults
    String link
    Map computeLinks
    boolean repeatLink = false
    String rangeEntityName
    List splitLinkRules
    Map construct = [:]
    Map<String, MarcSubFieldHandler> subfields = [:]
    List matchRules = []
    Map pendingResources

    static GENERIC_REL_URI_TEMPLATE = UriTemplate.fromTemplate("generic:{_}")

    MarcFieldHandler(conversion, tag, fieldDfn) {
        super(conversion, tag)
        ind1 = fieldDfn.i1? new MarcSubFieldHandler(this, "ind1", fieldDfn.i1) : null
        ind2 = fieldDfn.i2? new MarcSubFieldHandler(this, "ind2", fieldDfn.i2) : null
        pendingResources = fieldDfn.pendingResources

        dependsOn = fieldDfn.dependsOn

        if (fieldDfn.definesDomainEntity) {
            // implies no links, no range
            definesDomainEntityType = fieldDfn.definesDomainEntity
            domainEntityName = 'Instance'
        } else {
            if (fieldDfn.promoteToDomainEntity) {
                definesDomainEntityType = fieldDfn.promoteToDomainEntity
                domainEntityName = 'Instance'
            } else {
                domainEntityName = fieldDfn.domainEntity ?: 'Instance'
            }
            if (fieldDfn.addLink) {
                link = fieldDfn.addLink
                repeatLink = true
            } else {
                link = fieldDfn.link
            }
            rangeEntityName = fieldDfn.rangeEntity
        }

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
                repeatLink: 'addLink' in it]
        }

        fieldDfn.construct.each { prop, tplt ->
            construct[prop] = new StringConstruct(tplt)
        }

        def matchDomain = fieldDfn['match-domain']
        if (matchDomain) {
            matchRules << new DomainMatchRule(this, fieldDfn, matchDomain)
        }
        def matchI1 = fieldDfn['match-i1']
        if (matchI1) {
            matchRules << new IndMatchRule(this, fieldDfn, matchI1, 'i1')
        }
        def matchI2 = fieldDfn['match-i2']
        if (matchI2) {
            matchRules << new IndMatchRule(this, fieldDfn, matchI2, 'i2')
        }
        def matchCode = fieldDfn['match-code']
        if (matchCode) {
            matchRules << new CodeMatchRule(this, fieldDfn, matchCode)
        }
        def matchPattern = fieldDfn['match-pattern']
        if (matchPattern) {
            matchRules << new CodePatternMatchRule(this, fieldDfn, matchPattern)
        }
        fieldDfn.each { key, obj ->
            def m = key =~ /^\$(\w+)$/
            if (m) {
                addSubfield(m[0][1], obj)
            }
        }
        if (splitLinkRules) {
            assert rangeEntityName, "splitLinks requires rangeEntity in: ${fieldDfn}"
        }
    }

    void addSubfield(code, dfn) {
        subfields[code] = dfn? new MarcSubFieldHandler(this, code, dfn) : null
    }

    boolean convert(sourceMap, value, entityMap) {

        if (!(value instanceof Map)) {
            throw new MalformedFieldValueException()
        }

        def domainEntity = entityMap[domainEntityName]
        if (domainEntity == null) return false

        if (definesDomainEntityType) {
            domainEntity['@type'] = definesDomainEntityType
        }

        for (rule in matchRules) {
            def handler = rule.getHandler(domainEntity, value)
            if (handler) {
                // TODO: resolve combined config
                return handler.convert(sourceMap, value, entityMap)
            }
        }

        def entity = domainEntity

        def handled = new HashSet()

        def linkage = computeLinkage(entity, value, handled)
        if (linkage.newEntity) {
            entity = linkage.newEntity
        }

        def uriTemplateParams = [:]

        def unhandled = new HashSet()

        def localEntites = [:]

        // TODO: track unhandled indicators
        if (ind1)
            ind1.convertSubValue(value.ind1, entity, uriTemplateParams, localEntites)
        if (ind2)
            ind2.convertSubValue(value.ind2, entity, uriTemplateParams, localEntites)

        value.subfields.each {
            it.each { code, subVal ->
                def subDfn = subfields[code]
                def ok = false
                if (subDfn) {
                    def ent = (subDfn.domainEntityName)?
                        entityMap[subDfn.domainEntityName] :
                        (linkage.codeLinkSplits[code] ?: entity)
                    ok = subDfn.convertSubValue(subVal, ent, uriTemplateParams, localEntites)
                }
                if (!ok && !handled.contains(code)) {
                    unhandled << code
                }
            }
        }

        if (uriTemplate) {
            uriTemplateDefaults.each { k, v ->
                if (!uriTemplateParams.containsKey(k)) {
                    v = getByPath(domainEntity, k) ?: v
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
                addValue(domainEntity, it.rule.link, it.entity, it.rule.repeatLink)
            }
        }

        if (construct) {
            construct.each { prop, tplt ->
                def v = tplt.expand(entity)
                if (v) entity[prop] = v
            }
        }

        return unhandled.size() == 0
    }

    def computeLinkage(entity, value, handled) {
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
                    newEnt = ["@type": rangeEntityName]
                }
                splitResults << [rule: rule, entity: newEnt]
                rule.codes.each {
                    codeLinkSplits[it] = newEnt
                }
            }
        }
        def newEnt = null
        if ((!splitResults || spliceEntity) && rangeEntityName) {
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

            newEnt = newEntity(rangeEntityName)

            def lRepeatLink = repeatLink
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
                    newEnt['@id'] = "_:t-${UUID.randomUUID()}" as String
                entRef = ['@id': newEnt['@id']]
            }
            if (link) {
                addValue(entity, link, newEnt, lRepeatLink)
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

    Map getLocalEntity(Map owner, String id, Map localEntites) {
        def entity = localEntites[id]
        if (entity == null) {
            def pending = pendingResources[id]
            entity = localEntites[id] = newEntity(pending.rangeEntity)
            def link = pending.link ?: pending.addLink
            addValue(owner, link, entity, pending.containsKey('addLink'))
        }
        return entity
    }

    def revert(Map data) {
        def entity = getEntity(data)
        def entities = [entity]
        if (link) {
            entities = entity[link]
            if (!(entities instanceof List)) // should be if repeat == true
                entities = [entities]
            if (rangeEntityName) {
                entities = entities.findAll {
                    if (!it) return false
                    it['_DEBUG_VIA'] = link
                    def type = it['@type']
                    return (type instanceof List)?
                        rangeEntityName in type : type == rangeEntityName
                }
            }
        }
        def results = entities.collect { revertOne(data, it) }.findAll()
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
                resultItems += linked.collect { revertOne(data, it, rule.codes) }
            }
            if (resultItems.size() /*&& !repeatLink*/) {
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
        return results
    }

    def revertOne(Map data, Map currentEntity, Set onlyCodes=null) {
        def i1 = ind1? ind1.revert(data, currentEntity) : null
        def i2 = ind2? ind2.revert(data, currentEntity) : null
        def subs = []
        subfields.collect { code, subhandler ->
            if (!subhandler)
                return
            if (onlyCodes && !onlyCodes.contains(code))
                return
            def value = subhandler.revert(data, currentEntity)
            if (value instanceof List) {
                value.each {
                    subs << [(code): it]
                }
            } else if (value != null) {
                subs << [(code): value]
            }
        }
        return subs.length? [ind1: i1, ind2: i2, subfields: subs] : null
    }
}

class MarcSubFieldHandler extends ConversionPart {

    MarcFieldHandler fieldHandler
    String code
    char[] interpunctionChars
    char[] surroundingChars
    String link
    String about
    boolean repeatLink
    String property
    boolean repeatProperty
    String rangeEntityName
    UriTemplate subUriTemplate
    Pattern splitValuePattern
    List<String> splitValueProperties
    String rejoin
    Map defaults

    MarcSubFieldHandler(fieldHandler, code, Map subDfn) {
        this.fieldHandler = fieldHandler
        this.code = code
        domainEntityName = subDfn.domainEntity
        interpunctionChars = subDfn.interpunctionChars?.toCharArray()
        surroundingChars = subDfn.surroundingChars?.toCharArray()
        super.setTokenMap(fieldHandler, subDfn)
        link = subDfn.link
        about = subDfn.about
        repeatLink = false
        if (subDfn.addLink) {
            repeatLink = true
            link = subDfn.addLink
        }
        property = subDfn.property
        repeatProperty = false
        if (subDfn.addProperty) {
            property = subDfn.addProperty
            repeatProperty = true
        }
        rangeEntityName = subDfn.rangeEntity
        if (subDfn.uriTemplate) {
            subUriTemplate = UriTemplate.fromTemplate(subDfn.uriTemplate)
        }
        if (subDfn.splitValuePattern) {
            // TODO: support repeatable?
            splitValuePattern = Pattern.compile(subDfn.splitValuePattern)
            splitValueProperties = subDfn.splitValueProperties
            rejoin = subDfn.rejoin
        }
        defaults = subDfn.defaults
    }

    boolean convertSubValue(subVal, ent, uriTemplateParams, localEntites) {
        def ok = false
        def uriTemplateKeyBase = ""

        if (subVal)
            subVal = clearChars(subVal)

        if (tokenMap) {
            subVal = tokenMap[subVal]
            if (subVal == null)
                return false
        }

        if (about) {
            ent = fieldHandler.getLocalEntity(ent, about, localEntites)
        }

        if (link) {
            def entId = null
            if (subUriTemplate) {
                entId = subUriTemplate.expand(["_": subVal])
            }
            def newEnt = fieldHandler.newEntity(rangeEntityName, entId)
            fieldHandler.addValue(ent, link, newEnt, repeatLink)
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
            defaults.each { k, v -> if (!(k in ent)) ent[k] = v }
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
        def entity = domainEntityName?
            getEntity(data) : currentEntity
        // TODO: taking list[0] not enough – need to at least produce result list
        if (entity == null)
            return null
        if (link) {
            entity = entity[link]
            if (entity instanceof List) entity = entity[0]
        }
        if (entity == null)
            return null
        // TODO: match defaults only if not set by other subfield...
        if (defaults && defaults.any { p, o -> entity[p] != o })
            return null
        if (splitValueProperties && rejoin) {
            // TODO: and properties not all corresponding to codes...
            def vs = splitValueProperties.collect { entity[it] }.findAll()
            // TODO: revert splitValuePattern, check if prop
            if (vs.size() == splitValueProperties.size())
                return vs.join(rejoin)
        }
        if (property) {
            return revertObject(entity[property])
        }
        return null
    }

}

abstract class MatchRule {
    Map ruleMap = [:]
    MatchRule(handler, fieldDfn, rules) {
        rules.each { key, matchDfn ->
            def comboDfn = fieldDfn + matchDfn
            ['match-domain', 'match-i1', 'match-i2', 'match-code', 'match-pattern'].each {
                comboDfn.remove(it)
            }
            def tag = null
            ruleMap[key] = new MarcFieldHandler(handler.conversion, tag, comboDfn)
        }
    }
    MarcFieldHandler getHandler(entity, value) {
        return ruleMap[getKey(entity, value)]
    }
    abstract String getKey(entity, value)
}

class DomainMatchRule extends MatchRule {
    DomainMatchRule(handler, fieldDfn, rules) {
        super(handler, fieldDfn, rules)
    }
    String getKey(entity, value) {
        def type = entity['@type']
        if (type instanceof List)
            return type[-1]
        return type
    }
}

class IndMatchRule extends MatchRule {
    String indKey
    IndMatchRule(handler, fieldDfn, rules, indKey) {
        super(handler, fieldDfn, rules)
        this.indKey = indKey
    }
    String getKey(entity, value) {
        return value[indKey]
    }
}

class CodeMatchRule extends MatchRule {
    CodeMatchRule(handler, fieldDfn, rules) {
        super(handler, fieldDfn, parseRules(rules))
    }
    static Map parseRules(Map rules) {
        def parsed = [:]
        rules.each { key, map ->
            key.split().each { parsed[it] = map }
        }
        return parsed
    }
    String getKey(entity, value) {
        for (sub in value.subfields) {
            for (key in sub.keySet()) {
                if (key in ruleMap)
                    return key
            }
        }
    }
}

class CodePatternMatchRule extends MatchRule {
    Map<String, Map> patterns = [:]
    CodePatternMatchRule(handler, fieldDfn, rules) {
        super(handler, fieldDfn, parseRules(rules))
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
    String getKey(entity, value) {
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
}

class MalformedFieldValueException extends RuntimeException {}

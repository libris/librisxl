package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import java.util.regex.Pattern
import org.codehaus.jackson.map.ObjectMapper

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.basic.BasicFormatConverter

import static se.kb.libris.conch.Tools.*

import com.damnhandy.uri.template.UriTemplate


@Log
class MarcFrameConverter extends BasicFormatConverter {

    MarcConversion conversion
    def mapper = new ObjectMapper()

    MarcFrameConverter() {
        def loader = getClass().classLoader
        def config = loader.getResourceAsStream("marcframe.json").withStream {
            mapper.readValue(it, Map)
        }
        def resourceMaps = [:]
        config.resourceMaps.each { key, sourceRef ->
            if (sourceRef instanceof String) {
                resourceMaps[key] = loader.getResourceAsStream(sourceRef).withStream {
                    mapper.readValue(it, List).collectEntries { [it.code, it] }
                }
            } else {
                resourceMaps[key] = sourceRef
            }
        }
        conversion = new MarcConversion(config, resourceMaps)
    }

    Map createFrame(Map marcSource) {
        return conversion.createFrame(marcSource)
    }

    @Override
    String getResultContentType() { "application/ld+json" }

    @Override
    String getRequiredContentType() { "application/x-marc-json" }

    @Override
    Document doConvert(final Document doc) {
        def source = getDataAsMap(doc)
        def result = createFrame(source)
        log.trace("Created frame: $result")

        return new Document().withIdentifier(doc.identifier).withData(mapper.writeValueAsBytes(result)).withEntry(doc.entry).withMeta(doc.meta).withContentType("application/ld+json")
    }

    public static void main(String[] args) {
        def converter = new MarcFrameConverter()
        def fpath = args[0]
        def cmd = null
        if (args.length > 1) {
            cmd = args[0]
            fpath = args[1]
        }
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
    def typeTree = [:]
    Map resourceMaps
    Set primaryTags = new HashSet()

    MarcConversion(Map config, Map resourceMaps) {
        marcTypeMap = config.marcTypeFromTypeOfRecord
        this.resourceMaps = resourceMaps
        buildTypeTree(config.entityTypeMap)
        ['bib', 'auth', 'hold'].each {
            buildHandlers(config, it)
        }
    }

    void buildTypeTree(Map typeMap) {
        typeMap.each { type, rule ->
            if (!rule.typeOfRecord) return
            def recTypeTree = typeTree.get(rule.typeOfRecord, [:])
            def workTree = null
            if (rule.bibLevel) {
                workTree = recTypeTree[rule.bibLevel] = [type: type]
            } else {
                workTree = recTypeTree['*'] = [type: type]
            }
            if (rule.instanceTypes) {
                def instanceTree = workTree.instanceTree = [:]
                rule.instanceTypes.each { itype, irule ->
                    def carrierTree = instanceTree.get(irule.carrierType, [:])
                    if (irule.carrierMaterial) {
                        carrierTree[irule.carrierMaterial] = itype
                    } else {
                        carrierTree['*'] = itype
                    }
                }
            }
        }
    }

    void computeTypes(Map entityMap) {
        // TODO: remove fixed field remnants once they're subsumed into type?
        def record = entityMap.Record
        def recTypeTree = typeTree[record.typeOfRecord]
        if (!recTypeTree)
            return // missing concrete type mapping
        def workTree = recTypeTree[record.bibLevel] ?: recTypeTree['*']
        def workType = workTree?.type
        def instanceType = null
        if (workTree?.instanceTree) {
            def carrierTree = workTree.instanceTree[record.carrierType]
            if (carrierTree)
                instanceType = carrierTree[record.carrierMaterial] ?: carrierTree['*']
        }
        entityMap.Work['@type'] = workType ?: "Work"
        def instance = entityMap.Instance
        if (instanceType) {
            instance['@type'] = instanceType
        } else if (workType) {
            // TODO: is this sound, or are work types "non-manifestations"?
            instance['@type'] = workType
        } else {
            instance['@type'] = "Instance"
        }
    }

    String getMarcCategory(marcSource) {
        def typeOfRecord = marcSource.leader.substring(6, 7)
        return marcTypeMap[typeOfRecord] ?: marcTypeMap['*']
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
            if (fieldDfn.find { it.key[0] == '[' }) {
                handler = new MarcFixedFieldHandler(fieldDfn)
            } else if (fieldDfn.find { it.key[0] == '$' }) {
                handler = new MarcFieldHandler(tag, fieldDfn, resourceMaps)
                if (handler.dependsOn) {
                    primaryTags += handler.dependsOn
                }
                if (handler.definesDomainEntityType != null) {
                    primaryTags << tag
                }
            }
            else {
                handler = new MarcSimpleFieldHandler(tag, fieldDfn)
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

    Map createFrame(marcSource, recordId=null) {
        def unknown = []

        def record = ["@type": "Record", "@id": recordId]

        def entityMap = [Record: record]

        def marcCat = getMarcCategory(marcSource)
        def fieldHandlers = marcHandlers[marcCat]

        fieldHandlers["000"].convert(marcSource, marcSource.leader, entityMap)

        // TODO:
        // * always one record and a primary "thing"
        // * the type of this thing is determined during processing
        def work = [:]
        def instance = [:]
        entityMap['Instance'] = instance
        entityMap['Work'] = work

        record.about = instance

        def primaryFields = []
        def otherFields = []
        marcSource.fields.each { field ->
            def determinesType = false
            field.each { tag, value ->
                if (tag in PREPROC_TAGS && tag in fieldHandlers) {
                    fieldHandlers[tag].convert(marcSource, value, entityMap)
                } else if (tag in primaryTags) {
                    primaryFields << field
                } else {
                    otherFields << field
                }
            }
        }

        computeTypes(entityMap)

        // TODO: make this configurable
        if (record['@id'] == null) {
            record['@id'] = "/${marcCat}/${record.controlNumber}" as String
            instance['@id'] = "/resource/${marcCat}/${record.controlNumber}" as String
        }

        (primaryFields + otherFields).each { field ->
            def ok = false
            field.each { tag, value ->
                def handler = fieldHandlers[tag]
                if (handler) {
                    ok = handler.convert(marcSource, value, entityMap)
                }
            }
            if (!ok) {
                unknown << field
            }
        }
        if (unknown) {
            record.unknown = unknown
        }

        // TODO: only(?) bib (monographies), and use a config-defined link..
        if (work.find { k, v -> k != "@type" }) {
            instance['instanceOf'] = work
        }
        return record
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

class MarcFixedFieldHandler {

    def columns = []

    MarcFixedFieldHandler(fieldDfn) {
        fieldDfn.each { key, obj ->
            def m = (key =~ /^\[(\d+):(\d+)\]$/)
            if (m) {
                def start = m[0][1].toInteger()
                def end = m[0][2].toInteger()
                columns << new Column(obj, start, end, obj['default'])
            }
        }
    }

    boolean convert(marcSource, value, entityMap) {
        def success = true
        columns.each {
            if (!it.convert(marcSource, value, entityMap))
                success = false
        }
        return success
    }

    def revert(Map data) {
        def value = new StringBuilder(" " * (columns[-1].end + 1))
        for (col in columns) {
            def obj = col.revert(data)
            // TODO: ambiguity trouble if this is a List!
            if (obj instanceof List) obj = obj[0]
            if (obj) {
                value[col.start..col.end] = obj
            }
        }
        return value.toString()
    }

    class Column extends MarcSimpleFieldHandler {
        int start
        int end
        String defaultValue
        Column(fieldDfn, start, end, defaultValue) {
            super(null, fieldDfn)
            this.start = start
            this.end = end
            this.defaultValue = defaultValue
        }
        boolean convert(marcSource, value, entityMap) {
            def token = value.substring(start, end)
            if (token == " " || token == defaultValue)
                return true
            return super.convert(marcSource, token, entityMap)
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

    Map getEntity(Map data) {
        if (domainEntityName == 'Record')
            return data
        if (domainEntityName == 'Work')
            return data.about.instanceOf
        else
            return data.about
    }

}

abstract class BaseMarcFieldHandler extends ConversionPart {

    String tag
    BaseMarcFieldHandler(tag) { this.tag = tag }

    abstract boolean convert(marcSource, value, entityMap)

    abstract def revert(Map data)

    void addValue(obj, key, value, repeatable) {
        if (repeatable) {
            def l = obj[key] ?: []
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

    MarcSimpleFieldHandler(tag, fieldDfn) {
        super(tag)
        if (fieldDfn.addProperty) {
            property = fieldDfn.addProperty
            repeat = true
        } else {
            property = fieldDfn.property
        }
        domainEntityName = fieldDfn.domainEntity ?: 'Instance'
        dateTimeFormat = fieldDfn.parseDateTime
        ignored = fieldDfn.get('ignored', false)
        link = fieldDfn.link
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
    }

    boolean convert(marcSource, value, entityMap) {
        if (ignored || !(property || link))
            return
        if (dateTimeFormat) {
            value = Date.parse(dateTimeFormat, value).format(DT_FORMAT)
        }

        def ent = entityMap[domainEntityName]
        if (ent == null)
            return false
        if (link) {
            ent = ent[link] = newEntity(rangeEntityName)
        }
        if (uriTemplate) {
            if (!matchUriToken || matchUriToken.matcher(value).matches()) {
                ent['@id'] = uriTemplate.replace(URI_SLOT, value)
            } else {
                ent['@value'] = value
            }
        }
        else {
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
            if (dateTimeFormat)
                return Date.parse(DT_FORMAT, v).format(dateTimeFormat)
            return v
        } else {
            def id = entity instanceof Map? entity['@id'] : entity
            if (uriTemplate) {
                return extractToken(uriTemplate, id) ?: "N/A"
            }
            return "???"
        }
    }

    static String extractToken(tplt, value) {
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
    Map resourceMaps
    List matchRules = []

    static GENERIC_REL_URI_TEMPLATE = UriTemplate.fromTemplate("generic:{_}")

    MarcFieldHandler(tag, fieldDfn, resourceMaps) {
        super(tag)
        this.resourceMaps = resourceMaps
        ind1 = fieldDfn.i1? new MarcSubFieldHandler(this, "ind1", fieldDfn.i1) : null
        ind2 = fieldDfn.i2? new MarcSubFieldHandler(this, "ind2", fieldDfn.i2) : null

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
            matchRules << new DomainMatchRule(fieldDfn, matchDomain, resourceMaps)
        }
        def matchI1 = fieldDfn['match-i1']
        if (matchI1) {
            matchRules << new IndMatchRule(fieldDfn, matchI1, 'i1', resourceMaps)
        }
        def matchI2 = fieldDfn['match-i2']
        if (matchI2) {
            matchRules << new IndMatchRule(fieldDfn, matchI2, 'i2', resourceMaps)
        }
        def matchCode = fieldDfn['match-code']
        if (matchCode) {
            matchRules << new CodeMatchRule(fieldDfn, matchCode, resourceMaps)
        }
        def matchPattern = fieldDfn['match-pattern']
        if (matchPattern) {
            matchRules << new CodePatternMatchRule(fieldDfn, matchPattern, resourceMaps)
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

    boolean convert(marcSource, value, entityMap) {

        def domainEntity = entityMap[domainEntityName]
        if (!domainEntity) return false

        if (definesDomainEntityType) {
            domainEntity['@type'] = definesDomainEntityType
        }

        for (rule in matchRules) {
            def handler = rule.getHandler(domainEntity, value)
            if (handler) {
                // TODO: resolve combined config
                return handler.convert(marcSource, value, entityMap)
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

        // TODO: track unhandled indicators
        if (ind1)
            ind1.convertSubValue(value.ind1, entity, uriTemplateParams)
        if (ind2)
            ind2.convertSubValue(value.ind2, entity, uriTemplateParams)

        value.subfields.each {
            it.each { code, subVal ->
                def subDfn = subfields[code]
                def ok = false
                if (subDfn) {
                    def ent = (subDfn.domainEntityName)?
                        entityMap[subDfn.domainEntityName] :
                        (linkage.codeLinkSplits[code] ?: entity)
                    ok = subDfn.convertSubValue(subVal, ent, uriTemplateParams)
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
            // for multiply linked entities.
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
                        computeLinks.mapping : resourceMaps[computeLinks.mapping]
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

    def revert(Map data) {
        def entity = getEntity(data)
        def linkedEntities = null
        if (link) {
            linkedEntities = entity[link]
            if (!(linkedEntities instanceof List)) // should be if repeat == true
                linkedEntities = [linkedEntities]
        }
        def entities = linkedEntities ?: [entity]
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
                for (map in resultItems[1..-1]) {
                    merged.subfields += map.subfields
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
    Map valueMap
    String link
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
        def valueMap = subDfn.valueMap
        this.valueMap = (valueMap instanceof String)?
                            fieldHandler.resourceMaps[valueMap] : valueMap
        link = subDfn.link
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

    boolean convertSubValue(subVal, ent, uriTemplateParams) {
        def ok = false
        def uriTemplateKeyBase = ""

        if (subVal)
            subVal = clearChars(subVal)

        if (valueMap) {
            subVal = valueMap[subVal]
            if (subVal == null)
                return false
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
                    if (val[-1].equals(c.toString())) {
                        val = val[0..-2].trim()
                    }
                }
            }
            if (surroundingChars) {
                for (c in surroundingChars) {
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
            return entity[property]
        }
    }

}

abstract class MatchRule {
    Map ruleMap = [:]
    MatchRule(fieldDfn, rules, resourceMaps) {
        rules.each { key, matchDfn ->
            def comboDfn = fieldDfn + matchDfn
            ['match-domain', 'match-i1', 'match-i2', 'match-code', 'match-pattern'].each {
                comboDfn.remove(it)
            }
            def tag = null
            ruleMap[key] = new MarcFieldHandler(tag, comboDfn, resourceMaps)
        }
    }
    MarcFieldHandler getHandler(entity, value) {
        return ruleMap[getKey(entity, value)]
    }
    abstract String getKey(entity, value)
}

class DomainMatchRule extends MatchRule {
    DomainMatchRule(fieldDfn, rules, resourceMaps) {
        super(fieldDfn, rules, resourceMaps)
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
    IndMatchRule(fieldDfn, rules, indKey, resourceMaps) {
        super(fieldDfn, rules, resourceMaps)
        this.indKey = indKey
    }
    String getKey(entity, value) {
        return value[indKey]
    }
}

class CodeMatchRule extends MatchRule {
    CodeMatchRule(fieldDfn, rules, resourceMaps) {
        super(fieldDfn, parseRules(rules), resourceMaps)
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
    CodePatternMatchRule(fieldDfn, rules, resourceMaps) {
        super(fieldDfn, parseRules(rules), resourceMaps)
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

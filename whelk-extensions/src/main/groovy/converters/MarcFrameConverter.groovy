package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import java.util.regex.Pattern
import org.codehaus.jackson.map.ObjectMapper

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.basic.BasicFormatConverter

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
    String getRequiredContentType() { "application/x-marc-json" }

    @Override
    Document doConvert(Document doc) {
        def source = doc.dataAsMap
        def result = createFrame(source)
        log.trace("Created frame: $result")
        return doc.withData(mapper.writeValueAsBytes(result)).withContentType("application/ld+json")
    }

    public static void main(String[] args) {
        def converter = new MarcFrameConverter()
        def source = converter.mapper.readValue(new File(args[0]), Map)
        def frame = converter.createFrame(source)
        converter.mapper.writeValue(System.out, frame)
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

}

class MarcFixedFieldHandler {

    def columns = []

    MarcFixedFieldHandler(fieldDfn) {
        fieldDfn.each { key, obj ->
            def m = (key =~ /^\[(\d+):(\d+)\]$/)
            if (m) {
                def start = m[0][1].toInteger()
                def end = m[0][2].toInteger()
                columns << new Column(
                        domainEntity: obj.domainEntity, property: obj.property,
                        start: start, end: end, defaultValue: obj['default'])
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

    class Column {
        String domainEntity
        String property
        int start
        int end
        String defaultValue
        boolean convert(marcSource, value, entityMap) {
            def token = value.substring(start, end)
            if (token == " " || token == defaultValue)
                return true
            def entity = entityMap[domainEntity]
            if (entity == null)
                return false
            entity[property] = token
            return true
        }
    }

}

abstract class BaseMarcFieldHandler {

    String tag
    BaseMarcFieldHandler(tag) { this.tag = tag }

    abstract boolean convert(marcSource, value, entityMap)

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

    String property
    String domainEntityName
    String link
    String rangeEntityName
    String uriTemplate
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
        fieldDfn.get('ignored', false)
        link = fieldDfn.link
        rangeEntityName = fieldDfn.rangeEntity
        uriTemplate = fieldDfn.uriTemplate
    }

    boolean convert(marcSource, value, entityMap) {
        if (ignored)
            return
        if (dateTimeFormat) {
            value = Date.parse(dateTimeFormat, value).format("yyyy-MM-dd'T'HH:mm:ss.SZ")
        }

        def ent = entityMap[domainEntityName]
        if (link) {
            ent = ent[link] = newEntity(rangeEntityName)
        }
        if (uriTemplate)
            ent['@id'] = uriTemplate.replace('{_}', value)
        else
            addValue(ent, property, value, repeat)

        return true
    }

}

@Log
class MarcFieldHandler extends BaseMarcFieldHandler {

    Map ind1
    Map ind2
    List<String> dependsOn
    String domainEntityName
    String definesDomainEntityType
    UriTemplate uriTemplate
    Map uriTemplateDefaults
    String link
    Map computeLinks
    boolean repeatLink = false
    String rangeEntityName
    List splitLinkRules
    Map construct = [:]
    Map subfields = [:]
    Map resourceMaps
    List matchRules = []

    static GENERIC_REL_URI_TEMPLATE = UriTemplate.fromTemplate("generic:{_}")

    MarcFieldHandler(tag, fieldDfn, resourceMaps) {
        super(tag)
        ind1 = fieldDfn.i1
        ind2 = fieldDfn.i2

        dependsOn = fieldDfn.dependsOn

        if (fieldDfn.definesDomainEntity) {
            // implies no links, no range
            definesDomainEntityType = fieldDfn.definesDomainEntity
            domainEntityName = 'Instance'
        } else {
            domainEntityName = fieldDfn.domainEntity ?: 'Instance'
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
                repeatLink: 'addLink' in it]
        }

        fieldDfn.construct.each { prop, tplt ->
            construct[prop] = new StringConstruct(tplt)
        }
        this.resourceMaps = resourceMaps

        def matchDomain = fieldDfn['match-domain']
        if (matchDomain) {
            matchRules << new DomainMatchRule(fieldDfn, matchDomain, resourceMaps)
        }
        def matchI1 = fieldDfn['match-i1']
        if (matchI1) {
            matchRules << new IndMatchRule(fieldDfn, matchI1, 'ind1', resourceMaps)
        }
        def matchI2 = fieldDfn['match-i2']
        if (matchI2) {
            matchRules << new IndMatchRule(fieldDfn, matchI2, 'ind1', resourceMaps)
        }
        def matchCode = fieldDfn['match-code']
        if (matchCode) {
            matchRules << new CodeMatchRule(fieldDfn, matchCode, resourceMaps)
        }
        fieldDfn.each { key, obj ->
            def m = key =~ /^\$(\w+)$/
            if (m) {
                addSubfield(m[0][1], obj)
            }
        }
        if (splitLinkRules) {
            assert rangeEntityName
        }
    }

    void addSubfield(code, obj) {
        subfields[code] = obj
        if (obj) assert subfields[code] instanceof Map
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

        def codeLinkSplits = [:]
        // TODO: clear unused codeLinkSplits afterwards..
        def splitLinkDomain = entity
        def splitLinks = []
        if (splitLinkRules) {
            splitLinkRules.each { rule ->
                def newEnt = ["@type": rangeEntityName]
                splitLinks << [rule: rule, entity: newEnt]
                rule.codes.each {
                    codeLinkSplits[it] = newEnt
                }
            }
        } else if (rangeEntityName) {
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

            def newEnt = newEntity(rangeEntityName)

            if (useLinks) {
                repeatLink = true
            }

            // TODO: use @id (existing or added bnode-id) instead of duplicating newEnt
            def entRef = newEnt
            if (useLinks && link) {
                if (!newEnt['@id'])
                    newEnt['@id'] = "_:t-${new Date().time}" as String
                entRef = ['@id': newEnt['@id']]
            }
            if (link) {
                addValue(entity, link, newEnt, repeatLink)
            }
            useLinks.each {
                addValue(entity, it, entRef, repeatLink)
            }
            entity = newEnt
        }

        def uriTemplateParams = [:]

        def unhandled = new HashSet()

        if (ind1)
            processSubData(ind1, value.ind1, entity, uriTemplateParams)
        if (ind2)
            processSubData(ind2, value.ind2, entity, uriTemplateParams)

        value.subfields.each {
            it.each { code, subVal ->
                def subDfn = subfields[code]
                def ok = false
                if (subDfn) {
                    def ent = (subDfn.domainEntity)?
                        entityMap[subDfn.domainEntity] : (codeLinkSplits[code] ?: entity)
                    ok = processSubData(subDfn, subVal, ent, uriTemplateParams)
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
                entity[altUriRel] = ['@id': computedUri]
            } else {
                if (entity['@id']) {
                    entity[altUriRel] = ['@id': entity['@id']] // TODO: ok as precaution?
                }
                entity['@id'] = computedUri
            }
        }

        splitLinks.each {
            if (it.entity.find { k, v -> k != "@type" }) {
                addValue(splitLinkDomain, it.rule.link, it.entity, it.rule.repeatLink)
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

    String clearChars(fromWhere, removeChars, subVal) {
        def val = subVal.trim()
        if (val.size() > 2) {
            def chars = removeChars.toCharArray()
            switch(fromWhere) {
                case "trailing":
                    for (c in chars) {
                        if (val[-1].equals(c.toString())) {
                            val = val[0..-2].trim()
                        }
                    }
                    break
                case "surrounding":
                    val
                    for (c in chars) {
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

    boolean processSubData(subDfn, subVal, ent, uriTemplateParams) {
        def ok = false
        def uriTemplateKeyBase = ""

        if (subDfn.interpunctionChars) {
            subVal = clearChars("trailing", subDfn.interpunctionChars, subVal)
        }
        if (subDfn.surroundingChars) {
            subVal = clearChars("surrounding", subDfn.surroundingChars, subVal)
        }

        def valueMap = subDfn.valueMap
        if (valueMap) {
            if (valueMap instanceof String) // TODO: resolve on init
                valueMap = resourceMaps[valueMap]
            subVal = valueMap[subVal]
            if (subVal == null)
                return false
        }

        def link = subDfn.link
        def linkRepeat = false
        if (subDfn.addLink) {
            linkRepeat = true
            link = subDfn.addLink
        }
        if (link) {
            def entId = null
            if (subDfn.uriTemplate) {
                // TODO: compile subfield definitions
                def subUriTemplate = UriTemplate.fromTemplate(subDfn.uriTemplate)
                entId = subUriTemplate.expand(["_": subVal])
            }
            def newEnt = newEntity(subDfn.rangeEntity, entId)
            addValue(ent, link, newEnt, linkRepeat)
            ent = newEnt
            uriTemplateKeyBase = "${link}."
        }

        def property = subDfn.property
        def repeat = false
        if (subDfn.addProperty) {
            property = subDfn.addProperty
            repeat = true
        }

        if (subDfn.pattern) {
            // TODO: support repeatable?
            def pattern = Pattern.compile(subDfn.pattern)
            def m = pattern.matcher(subVal)
            if (m) {
                subDfn.properties.eachWithIndex { prop, i ->
                    def v = m[0][i + 1]
                    if (v) {
                        ent[prop] = v
                    }
                    addValue(uriTemplateParams, uriTemplateKeyBase + prop, v, true)
                }
                ok = true
            }
        }
        if (!ok && property) {
            addValue(ent, property, subVal, repeat)
            addValue(uriTemplateParams, uriTemplateKeyBase + property, subVal, true)
            ok = true
        }

        if (subDfn.defaults) {
            subDfn.defaults.each { k, v -> if (!(k in ent)) ent[k] = v }
        }
        return ok
    }

}

abstract class MatchRule {
    Map ruleMap = [:]
    MatchRule(fieldDfn, rules, resourceMaps) {
        rules.each { key, matchDfn ->
            def comboDfn = fieldDfn + matchDfn
            ['match-domain', 'match-i1', 'match-i2', 'match-code'].each {
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

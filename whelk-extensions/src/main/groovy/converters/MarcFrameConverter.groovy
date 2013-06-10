package se.kb.libris.whelks.plugin

import java.util.regex.Pattern
import org.codehaus.jackson.map.ObjectMapper

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.basic.BasicDocument
import se.kb.libris.whelks.basic.BasicFormatConverter


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
            resourceMaps[key] = loader.getResourceAsStream(sourceRef).withStream {
                mapper.readValue(it, List).collectEntries { [it.code, it] }
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
        def source = doc.dataAsJson
        def result = createFrame(source)
        return new BasicDocument(doc)
            .withData(mapper.writeValueAsBytes(result))
            .withContentType("application/ld+json")
    }

    public static void main(String[] args) {
        def converter = new MarcFrameConverter()
        def source = converter.mapper.readValue(new File(args[0]), Map)
        def frame = converter.createFrame(source)
        converter.mapper.writeValue(System.out, frame)
    }

}


class MarcConversion {

    static FIXED_TAGS = ["000", "006", "007", "008"] as Set

    Map marcTypeMap = [:]
    def marcHandlers = [:]
    def typeTree = [:]
    Map resourceMaps

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
        def workType = workTree?.type ?: "Work"
        def instanceType = "Instance"
        if (workTree?.instanceTree) {
            def carrierTree = workTree.instanceTree[record.carrierType]
            if (carrierTree)
                instanceType = carrierTree[record.carrierMaterial] ?: carrierTree['*']
        }
        entityMap.Work['@type'] = workType
        if (instanceType)
            entityMap.Instance['@type'] = instanceType
    }

    String getMarcCategory(marcSource) {
        def typeOfRecord = marcSource.leader.substring(6, 7)
        return marcTypeMap[typeOfRecord] ?: marcTypeMap['*']
    }

    void buildHandlers(config, marcCategory) {
        def fieldHandlers = marcHandlers[marcCategory] = [:]
        def subConf = config[marcCategory]
        subConf.each { tag, fieldDfn ->
            def handler = null
            def m = null
            if (fieldDfn.inherit) {
                def refTag = tag
                def ref = fieldDfn.inherit
                if (ref.contains(':')) {
                    (ref, refTag) = ref.split(':')
                }
                def baseDfn =  (ref in subConf)? subConf[ref] : config[ref][refTag]
                fieldDfn = baseDfn + fieldDfn
            }
            if (fieldDfn.ignored || fieldDfn.size() == 0) {
                return
            }
            fieldDfn.each { key, obj ->
                if ((m = key =~ /^\[(\d+):(\d+)\]$/)) {
                    if (handler == null) {
                        handler = new MarcFixedFieldHandler()
                    }
                    def start = m[0][1].toInteger()
                    def end = m[0][2].toInteger()
                    handler.addColumn(obj.domainEntity, obj.property,
                            start, end, obj['default'])
                } else if ((m = key =~ /^\$(\w+)$/)) {
                    if (handler == null) {
                        handler = new MarcFieldHandler(fieldDfn, resourceMaps)
                    }
                    def code = m[0][1]
                    handler.addSubfield(code, obj)
                }
            }
            if (handler == null) {
                handler = new MarcSimpleFieldHandler(fieldDfn)
            }
            fieldHandlers[tag] = handler
        }
    }

    Map createFrame(marcSource) {
        def unknown = []

        def record = ["@type": "Record"]

        def entityMap = [Record: record]

        def fieldHandlers = marcHandlers[getMarcCategory(marcSource)]

        fieldHandlers["000"].convert(marcSource, marcSource.leader, entityMap)

        // TODO:
        // * always one record and a primary "thing"
        // * the type of this thing is determined during processing
        def work = [:]
        def instance = [
            describedby: record
        ]
        entityMap['Instance'] = instance
        entityMap['Work'] = work

        def otherFields = []
        marcSource.fields.each { field ->
            def isFixed = false
            field.each { tag, value ->
                isFixed = (tag in FIXED_TAGS)
                if (isFixed)
                    fieldHandlers[tag].convert(marcSource, value, entityMap)
            }
            if (!isFixed)
                otherFields << field
        }

        computeTypes(entityMap)

        otherFields.each { field ->
            def ok = false
            field.each { tag, value ->
                if (tag in FIXED_TAGS) return // handled above
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
            instance.unknown = unknown
        }

        // TODO: only(?) bib (monographies), and use a config-defined link..
        if (work.find { k, v -> k != "@type" }) {
            instance['instanceOf'] = work
        }
        return instance
    }

}

class MarcFixedFieldHandler {
    def columns = []
    void addColumn(domainEntity, property, start, end, defaultValue=null) {
        columns << new Column(
                domainEntity: domainEntity, property: property,
                start: start, end: end, defaultValue: defaultValue)
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

    abstract boolean convert(marcSource, value, entityMap)

    void addValue(obj, key, value, repeatable) {
        if (repeatable) {
            def l = obj[key] ?: []
            l << value
            value = l
        }
        obj[key] = value
    }

}

class MarcSimpleFieldHandler extends BaseMarcFieldHandler {
    String property
    String domainEntityName
    boolean repeat = false
    String dateTimeFormat
    boolean ignored = false
    MarcSimpleFieldHandler(fieldDfn) {
        if (fieldDfn.addProperty) {
            property = fieldDfn.addProperty
            repeat = true
        } else {
            property = fieldDfn.property
        }
        domainEntityName = fieldDfn.domainEntity ?: 'Instance'
        dateTimeFormat = fieldDfn.parseDateTime
        fieldDfn.get('ignored', false)
    }
    boolean convert(marcSource, value, entityMap) {
        if (ignored)
            return
        assert property, value
        // TODO: handle repeatable
        if (dateTimeFormat) {
            value = Date.parse(dateTimeFormat, value).format("yyyy-MM-dd'T'HH:mm:ss.SZ")
        }
        entityMap[domainEntityName][property] = value
        return true
    }
}

class MarcFieldHandler extends BaseMarcFieldHandler {

    String ind1
    String ind2
    String domainEntityName
    String resetDomainEntityName
    String link
    Map computeLinks
    boolean repeatLink = false
    String rangeEntityName
    List splitLinkRules
    Map construct = [:]
    Map subfields = [:]
    Map resourceMaps
    List matchRules = []

    MarcFieldHandler(fieldDfn, resourceMaps) {
        ind1 = fieldDfn.i1
        ind2 = fieldDfn.i2

        if (fieldDfn.resetDomainEntity) {
            // implies no links, no range
            resetDomainEntityName = fieldDfn.resetDomainEntity
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

        computeLinks = fieldDfn.computeLinks?.clone()
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
        // TODO: this hardwires subfields; matches could possibly redefine those(?)
        matchRules.each {
            it.ruleMap.values().each {
                it.subfields = this.subfields
            }
        }
    }

    void addSubfield(code, obj) {
        subfields[code] = obj
    }

    boolean convert(marcSource, value, entityMap) {

        def entity = entityMap[domainEntityName]
        if (!entity) return false

        for (rule in matchRules) {
            def handler = rule.getHandler(entity, value)
            if (handler) {
                // TODO: resolve combined config
                return handler.convert(marcSource, value, entityMap)
            }
        }

        if (resetDomainEntityName) {
            entity['@type'] = resetDomainEntityName
        }

        def codeLinkSplits = [:]
        // TODO: clear unused codeLinkSplits afterwards..
        def splitLinkDomain = entity
        def splitLinks = []
        if (splitLinkRules) {
            assert rangeEntityName
            splitLinkRules.each { rule ->
                def newEnt = ["@type": rangeEntityName]
                splitLinks << [rule: rule, entity: newEnt]
                rule.codes.each {
                    codeLinkSplits[it] = newEnt
                }
            }
        } else if (rangeEntityName) {
            // TODO: mark used subfield as handled
            def useLinks = Collections.emptyList()
            if (computeLinks) {
                def use = computeLinks.use
                def resourceMap = resourceMaps[computeLinks.mapping]
                def linkTokens = value.subfields.findAll {
                    use in it.keySet() }.collect { it.iterator().next().value }
                useLinks = linkTokens.collect { resourceMap[it]?.term ?: "involved_as_${it}" }
            }
            if (!useLinks && link) {
                useLinks = [link]
            }

            def newEnt = ["@type": rangeEntityName]
            // TODO: use @id (existing or added bnode-id) instead of duplicating newEnt
            useLinks.each {
                addValue(entity, it, newEnt, repeatLink)
            }
            entity = newEnt
        }

        def unhandled = []

        value.subfields.each {
            it.each { code, subVal ->
                def subDfn = subfields[code]
                def handled = false
                if (subDfn) {
                    def ent = (subDfn.domainEntity)?
                        entityMap[subDfn.domainEntity] : (codeLinkSplits[code] ?: entity)
                    if (subDfn.link) {
                        ent = ent[subDfn.link] = ["@type": subDfn.rangeEntity]
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
                                if (v) ent[prop] = v
                            }
                            handled = true
                        }
                    }
                    if (!handled && property) {
                        addValue(ent, property, subVal, repeat)
                        handled = true
                    }
                    if (subDfn.defaults) {
                        subDfn.defaults.each { k, v -> if (!(k in ent)) ent[k] = v }
                    }
                }
                if (!handled) {
                    unhandled << code
                }
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

}

abstract class MatchRule {
    Map ruleMap = [:]
    MatchRule(fieldDfn, rules, resourceMaps) {
        rules.each { key, matchDfn ->
            def comboDfn = fieldDfn + matchDfn
            ['match-domain', 'match-i1', 'match-i2', 'match-code'].each {
                comboDfn.remove(it)
            }
            ruleMap[key] = new MarcFieldHandler(comboDfn, resourceMaps)
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

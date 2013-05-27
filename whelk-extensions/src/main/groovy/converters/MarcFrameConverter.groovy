package se.kb.libris.whelks.plugin

import java.util.regex.Pattern
import org.codehaus.jackson.map.ObjectMapper


class MarcFrameConverter {

    MarcConversion conversion
    def mapper = new ObjectMapper()

    MarcFrameConverter() {
        def loader = getClass().classLoader
        def config = loader.getResourceAsStream("marcframe.json").withStream {
            mapper.readValue(it, Map)
        }
        def relators = loader.getResourceAsStream("relatorcodes.json").withStream {
            mapper.readValue(it, List).collectEntries { [it.code, it] }
        }
        conversion = new MarcConversion(config, relators)
    }

    Map createFrame(Map marcSource) {
        return conversion.createFrame(marcSource)
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
    Map relators

    MarcConversion(Map config, Map relators) {
        marcTypeMap = config.marcTypeFromTypeOfRecord
        this.relators = relators

        buildTypeTree(config.entityTypeMap)
        buildHandlers(config, 'bib')

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
                        recTypeTree['*'] = itype
                    }
                }
            }
        }
    }

    void computeTypes(Map entityMap) {
        def record = entityMap.Record
        def recTypeTree = typeTree[record.typeOfRecord]
        def workTree = recTypeTree[record.bibLevel] ?: recTypeTree['*']
        def workType = workTree.type
        def instanceType = workType // TODO: is this logically sound?
        if (workTree.instanceTree) {
            def carrierTree = workTree.instanceTree[record.carrierType]
            if (carrierTree)
                instanceType = carrierTree[record.carrierMaterial] ?: carrierTree['*']
        }
        entityMap.Work['@type'] = [entityMap.Work['@type'], workType]
        if (instanceType)
            entityMap.Instance['@type'] = [entityMap.Instance['@type'], instanceType]
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
                fieldDfn = subConf[fieldDfn.inherit] + fieldDfn
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
                    handler.addColumn(obj.domainEntity, obj.property, start, end)
                } else if ((m = key =~ /^\$(\w+)$/)) {
                    if (handler == null) {
                        handler = new MarcFieldHandler(fieldDfn)
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

        def work = ["@type": "Work"]
        def instance = [
            "@type": "Instance",
            instanceOf: work,
            describedby: record,
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
        return instance
    }

}

class MarcFixedFieldHandler {
    def columns = []
    void addColumn(domainEntity, property, start, end) {
        columns << new Column(domainEntity: domainEntity, property: property, start: start, end: end)
    }
    boolean convert(marcSource, value, entityMap) {
        columns.each {
            it.convert(marcSource, value, entityMap)
        }
        return true
    }
    class Column {
        String domainEntity
        String property
        int start
        int end
        boolean convert(marcSource, value, entityMap) {
            def token = value.substring(start, end)
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
    MarcSimpleFieldHandler(fieldDfn) {
        if (fieldDfn.addProperty) {
            property = fieldDfn.addProperty
            repeat = true
        } else {
            property = fieldDfn.property
        }
        domainEntityName = fieldDfn.domainEntity ?: 'Instance'
    }
    boolean convert(marcSource, value, entityMap) {
        assert property, value
        // TODO: handle repeatable
        entityMap[domainEntityName][property] = value
        return true
    }
}

class MarcFieldHandler extends BaseMarcFieldHandler {
    String ind1
    String ind2
    String domainEntityName
    String link
    boolean repeatLink = false
    String rangeEntityName
    List splitLinkRules
    Map create
    Map subfields = [:]
    MarcFieldHandler(fieldDfn) {
        ind1 = fieldDfn.i1
        ind2 = fieldDfn.i2
        domainEntityName = fieldDfn.domainEntity ?: 'Instance'
        if (fieldDfn.addLink) {
            link = fieldDfn.addLink
            repeatLink = true
        } else {
            link = fieldDfn.link
        }
        // TODO: handle link via subfield! (using relators)
        rangeEntityName = fieldDfn.rangeEntity
        splitLinkRules = fieldDfn.splitLink.collect {
            [codes: new HashSet(it.codes),
                link: it.link ?: it.addLink,
                repeatLink: 'addLink' in it]
        }
        create = fieldDfn.create
    }
    void addSubfield(code, obj) {
        subfields[code] = obj
    }
    boolean convert(marcSource, value, entityMap) {
        def entity = entityMap[domainEntityName]
        if (!entity) return false

        // TODO: clear unused codeLinkSplits afterwards..
        def codeLinkSplits = [:]
        if (splitLinkRules) {
            assert rangeEntityName
            splitLinkRules.each { rule ->
                def newEnt = ["@type": rangeEntityName]
                addValue(entity, rule.link, newEnt, rule.repeatLink)
                rule.codes.each {
                    codeLinkSplits[it] = newEnt
                }
            }
        } else if (rangeEntityName) {
            def newEnt = ["@type": rangeEntityName]
            if (repeatLink) {
                def entList = entity[link]
                if (entList == null) {
                    entList = entity[link] = []
                }
                entList << newEnt
            } else {
                entity[link] = newEnt
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
                    if (!handled && subDfn.property) {
                        addValue(ent, subDfn.property, subVal, repeat)
                        handled = true
                    }
                    if (subDfn.defaults) {
                        ent += subDfn.defaults
                    }
                }
                if (!handled) {
                    unhandled << code
                }
            }
        }

        if (create) {
            create.each { prop, rule ->
                def source = rule.source.collect { entity[it] ?: "" } as String[]
                def v = String.format(rule.format, source)
                if (v) entity[prop] = v
            }
        }

        return unhandled.size() == 0
    }

}

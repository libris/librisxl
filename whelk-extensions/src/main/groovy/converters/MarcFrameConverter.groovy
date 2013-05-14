package se.kb.libris.whelks.plugin

import org.codehaus.jackson.map.ObjectMapper


class MarcFrameConverter {

    MarcFrame mf
    def mapper = new ObjectMapper()

    MarcFrameConverter() {
        def loader = getClass().classLoader
        def config = loader.getResourceAsStream("marcframe.json").withStream {
            mapper.readValue(it, Map)
        }
        def relators = loader.getResourceAsStream("relatorcodes.json").withStream {
            mapper.readValue(it, List).collectEntries { [it.code, it] }
        }
        mf = new MarcFrame(config, relators)
    }

    Map createFrame(Map marcSource) {
        return mf.createFrame(marcSource)
    }

    String getMarcType(Map marcSource) {
        return mf.getMarcType(marcSource)
    }

    public static void main(String[] args) {
        def converter = new MarcFrameConverter()
        def source = converter.mapper.readValue(new File(args[0]), Map)
        def frame = converter.createFrame(source)
        converter.mapper.writeValue(System.out, frame)
    }

}


class MarcFrame {
    Map marcTypeMap = [:]
    Map marcConversions = [:]
    Map relators
    MarcFrame(Map config, Map relators) {
        marcTypeMap = config.marcTypeFromTypeOfRecord.clone()
        this.relators = relators
        marcConversions["bib"] = new MarcBibConversion(config["bib"])
        //marcConversions["auth"] = new MarcAuthConversion(config["auth"])
        //marcConversions["hold"] = new MarcHoldingsConversion(config["hold"])
    }

    Map createFrame(marcSource) {
        def marcType = getMarcType(marcSource)
        return marcConversions[marcType].createFrame(marcSource)
    }

    String getMarcType(marcSource) {
        def typeOfRecord = marcSource.leader.substring(6, 7)
        return marcTypeMap[typeOfRecord] ?: marcTypeMap['*']
    }

}

class BaseMarcConversion {

    def fieldHandlers = [:]
    //def entityHandlers = [:]

    BaseMarcConversion(data) {
        data.each { tag, fieldDfn ->
            def handler = null
            def m = null
            if (fieldDfn.inherit) {
                fieldDfn = data[fieldDfn.inherit] + fieldDfn
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
                    handler.addColumn(obj.id, start, end)
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
}

class MarcBibConversion extends BaseMarcConversion {

    MarcBibConversion(data) {
        super(data)
    }

    Map createFrame(marcSource) {
        def record = ["@type": "Record"]
        def work = ["@type": "Work"]
        def unknown = []
        def instance = [
            "@type": "Instance",
            instanceOf: work,
            describedby: record,
        ]
        def entityMap = [:]
        [record, instance, work].each {
            entityMap[it["@type"]] = it
        }
        fieldHandlers["000"].convert(marcSource, marcSource.leader, entityMap)
        marcSource.fields.each { row ->
            def ok = false
            row.each { tag, value ->
                def handler = fieldHandlers[tag]
                if (handler) {
                    ok = handler.convert(marcSource, value, entityMap)
                }
            }
            if (!ok) {
                unknown << row
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
    void addColumn(property, start, end) {
        columns << new Column(property: property, start: start, end: end)
    }
    boolean convert(marcSource, value, entityMap) {
        // TODO
        return false
    }
    class Column {
        String property
        int start
        int end
    }
}

class MarcSimpleFieldHandler {
    String property
    String domainEntityName
    MarcSimpleFieldHandler(fieldDfn) {
        property = fieldDfn.property
        domainEntityName = fieldDfn.domainEntity ?: 'Instance'
    }
    boolean convert(marcSource, value, entityMap) {
        assert property, value
        entityMap[domainEntityName][property] = value
        return true
    }
}

class MarcFieldHandler {
    String ind1
    String ind2
    String domainEntityName
    String link
    String rangeEntityName
    String property
    //Map partitionRules
    Map subfields = [:]
    MarcFieldHandler(fieldDfn) {
        ind1 = fieldDfn.i1
        ind2 = fieldDfn.i2
        domainEntityName = fieldDfn.domainEntityName ?: 'Instance'
        link = fieldDfn.link
        rangeEntityName = fieldDfn.rangeEntity
        property = fieldDfn.property
        //partitionRules = fieldDfn.partition
    }
    void addSubfield(code, obj) {
        // TODO
        subfields[code] = obj
    }
    boolean convert(marcSource, value, entityMap) {
        // TODO:
        //def partitions = [:]
        def entity = entityMap[domainEntityName]
        if (!entity) return false
        if (rangeEntityName) {
            entity = entity[link] = ["@type": rangeEntityName]
        }
        def unhandled = []
        value.subfields.each {
            it.each { code, subVal ->
                def subDfn = subfields[code]
                if (subDfn) {
                    def ent = (subDfn.domainEntity)? entityMap[subDfn.domainEntity] : entity
                    if (subDfn.link) {
                        ent = ent[subDfn.link] = ["@type": subDfn.rangeEntity]
                    }
                    assert subDfn.property
                    ent[subDfn.property] = subVal
                    if (subDfn.defaults) {
                        ent += subDfn.defaults
                    }
                } else {
                    unhandled << code
                }
            }
        }
        return unhandled.size() == 0
    }
}

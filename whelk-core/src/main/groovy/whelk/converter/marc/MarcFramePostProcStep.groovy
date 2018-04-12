package whelk.converter.marc

import java.util.regex.Pattern
import groovy.util.logging.Log4j2 as Log

interface MarcFramePostProcStep {
    def ID = '@id'
    def TYPE = '@type'
    void modify(Map record, Map thing)
    void unmodify(Map record, Map thing)
}


abstract class MarcFramePostProcStepBase implements MarcFramePostProcStep {
    String type
    Pattern matchValuePattern
    void setMatchValuePattern(String pattern) {
        matchValuePattern = Pattern.compile(pattern)
    }
}


class FoldLinkedPropertyStep extends MarcFramePostProcStepBase {

    String statusFlag
    String statusFlagValue
    String sourceProperty
    String property
    String defaultLink
    Map<String, String> typeLinkMap

    def getLink(thing) {
        def useLink = defaultLink
        typeLinkMap.each { type, link ->
            if ((thing[TYPE] instanceof List &&
                        thing[TYPE].find { it.startsWith(type) })
                    || thing[TYPE]?.startsWith(type)) {
                useLink = link
            }
        }
        return useLink
    }

    void modify(Map record, Map thing) {
        def link = getLink(thing)
        if (thing[statusFlag] != statusFlagValue)
            return
        def value = thing[sourceProperty]
        if (!(value instanceof String) || !matchValuePattern.matcher(value))
            return
        for (object in thing[link]) {
            if (object[property] == value) {
                thing.remove(statusFlag)
                thing.remove(sourceProperty)
                return
            }
        }
        if (thing[link]) {
            return
        }
        thing.remove(statusFlag)
        thing.remove(sourceProperty)
        thing[link] = []
        thing[link] << [(property): value]
    }

    void unmodify(Map record, Map thing) {
        if (thing[statusFlag])
            return
        if (thing[sourceProperty])
            return
        def link = getLink(thing)
        for (object in thing[link]) {
            def value = object[property]
            if (!(value instanceof String) || !matchValuePattern.matcher(value))
                continue
            thing[statusFlag] = statusFlagValue
            thing[sourceProperty] = value
            break
        }
    }

}


class FoldJoinedPropertiesStep extends MarcFramePostProcStepBase {
    static String INFINITY_YEAR = "9999"
    String statusFlag
    String statusFlagValue
    List<String> sourceProperties
    String property
    String separator

    void modify(Map record, Map thing) {
        if (thing[statusFlag] != statusFlagValue)
            return
        def values = []
        for (prop in sourceProperties) {
            def value = thing[prop]
            if (value && !matchValuePattern.matcher(value))
                return
            values << (value == INFINITY_YEAR? "" : value)
        }
        thing.remove(statusFlag)
        sourceProperties.each { thing.remove(it) }
        thing[property] = values.join(separator)
    }

    void unmodify(Map record, Map thing) {
        if (thing[statusFlag])
            return
        if (sourceProperties.find { thing[it] })
            return
        def value = thing[property]
        if (!value)
            return
        if (value.endsWith(separator))
            value += INFINITY_YEAR
        def values = value.split(separator)
        if (values.size() != sourceProperties.size())
            return
        values.eachWithIndex { v, i ->
            if (!matchValuePattern.matcher(v))
                return
            thing[sourceProperties[i]] = v
        }
    }

}


class MappedPropertyStep implements MarcFramePostProcStep {

    String type
    String sourceEntity
    String sourceLink
    String sourceProperty
    String targetEntity
    String targetProperty
    Map<String, String> valueMap

    /**
     * Sets computed value if missing. Leaves any given value as is.
     */
    void modify(Map record, Map thing) {
        def target = targetEntity == "?record"? record : thing
        if (target[targetProperty]) {
            return
        }

        def source = sourceEntity == "?record"? record : thing
        def values = source.get(sourceLink)?.get(sourceProperty)
        if (values instanceof String) {
            values = [values]
        }

        for (value in values) {
            def mapped = valueMap[value]
            if (mapped) {
                target[targetProperty] = mapped
                break
            }
        }
    }

    /**
     * Adds computed value by invoking {@link modify}.
     */
    void unmodify(Map record, Map thing) {
        modify(record, thing)
    }

}

class VerboseRevertDataStep extends MarcFramePostProcStepBase {

    String sourceProperty
    String addLink
    String property
    String addProperty

    void modify(Map record, Map thing) {
    }

    void unmodify(Map record, Map thing) {
        expand(thing)
    }

    void expand(Map entity) {
        entity.each { key, value ->
            if (value instanceof List) {
                for (item in value) {
                    if (item instanceof Map) {
                        expand(item)
                    }
                }
            } else if (value instanceof Map) {
                expand(value)
            }
        }
        String v = entity[sourceProperty]
        if (v != null) {
            Map owner = entity
            if (addLink) {
                owner = [:]
                entity.get(addLink, []) << owner
            }
            if (property)
                owner[property] = v
            else {
                def list = [v]
                owner[addProperty] = list
            }

        }
    }

}

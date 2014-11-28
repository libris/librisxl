package whelk.plugin

import java.util.regex.Pattern
import groovy.util.logging.Slf4j as Log

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
            if (thing[TYPE].find { it.startsWith(type) }) {
                useLink = link
            }
        }
        return useLink
    }

    void modify(Map record, Map thing) {
        def link = getLink(thing)
        if (thing[statusFlag]?.get(ID) != statusFlagValue)
            return
        def value = thing[sourceProperty]
        if (!matchValuePattern.matcher(value))
            return
        for (object in thing[link]) {
            if (object[property] == value) {
                thing.remove(statusFlag)
                thing.remove(sourceProperty)
                return
            }
        }
        thing[link] = [(property): value]
    }

    void unmodify(Map record, Map thing) {
        if (thing[statusFlag])
            return
        if (thing[sourceProperty])
            return
        def link = getLink(thing)
        for (object in thing[link]) {
            def value = object[property]
            if (!matchValuePattern.matcher(value))
                continue
            thing[statusFlag] = [(ID): statusFlagValue]
            thing[sourceProperty] = value
            break
        }
    }

}


class FoldJoinedPropertiesStep extends MarcFramePostProcStepBase {
    String statusFlag
    String statusFlagValue
    List<String> sourceProperties
    String property
    String separator

    void modify(Map record, Map thing) {
    }

    void unmodify(Map record, Map thing) {
    }

}

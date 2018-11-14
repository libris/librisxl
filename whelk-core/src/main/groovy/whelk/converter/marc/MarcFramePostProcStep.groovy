package whelk.converter.marc

import java.util.regex.Matcher
import java.util.regex.Pattern
import groovy.util.logging.Log4j2 as Log

import whelk.JsonLd

interface MarcFramePostProcStep {
    def ID = '@id'
    def TYPE = '@type'
    void setLd(JsonLd ld)
    void init()
    void modify(Map record, Map thing)
    void unmodify(Map record, Map thing)
}


abstract class MarcFramePostProcStepBase implements MarcFramePostProcStep {

    String type
    Pattern matchValuePattern
    JsonLd ld

    void setMatchValuePattern(String pattern) {
        matchValuePattern = Pattern.compile(pattern)
    }

    void init() { }

    def findValue(source, List path) {
        if (!source || !path) {
            return source
        }
        if (source instanceof List) {
            source = source[0]
        }
        return findValue(source[path[0]], path.size() > 1 ? path[1..-1] : null)
    }

    static String buildString(Map node, List showProperties) {
        def result = ""
        for (prop in showProperties) {
            def fmt = null
            if (prop instanceof Map) {
                fmt = prop.useValueFormat
                prop = prop.property
            }
            def value = node[prop]
            if (value) {
                if (!(value instanceof List)) {
                    value = [value]
                }
                def first = true
                if (fmt?.contentFirst) {
                    result += fmt.contentFirst
                }
                def prevAfter = null
                value.each {
                    if (prevAfter) {
                        result += prevAfter
                    }
                    if (fmt?.contentBefore && (!fmt.contentFirst || !first)) {
                        result += fmt.contentBefore
                    }
                    result += it
                    first = false
                    prevAfter = fmt?.contentAfter
                }
                if (fmt?.contentLast) {
                    result += fmt.contentLast
                } else if (prevAfter) {
                    result += prevAfter
                }
            } else if (fmt?.contentNoValue) {
                result += fmt.contentNoValue
            }
        }
        return result
    }

}


class MappedPropertyStep implements MarcFramePostProcStep {

    JsonLd ld
    String type
    String sourceEntity
    String sourceLink
    String sourceProperty
    String targetEntity
    String targetProperty
    Map<String, String> valueMap

    void init() { }

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


class RestructPropertyValuesAndFlagStep extends MarcFramePostProcStepBase {

    String sourceLink
    String overwriteType
    String flagProperty
    String fuzzyMergeProperty
    Pattern fuzzPattern
    Map magicValueParts

    Map<String, FlagMatchRule> flagMatch = [:]

    private Map<String, List<FlagMatchRule>> flagMatchByLink = [:]

    void setFuzzPattern(String pattern) {
        fuzzPattern = Pattern.compile(pattern)
    }

    void setFlagMatch(Map flagMatch) {
        flagMatch.each { flag, ruleDfn ->
            if (ruleDfn instanceof Map) {
                def flagRule = new FlagMatchRule(ruleDfn)
                flagRule.flag = flag
                this.flagMatch[(String) flag] = flagRule
                if (flagRule.mergeFirstLink) {
                    flagMatchByLink.get(flagRule.mergeFirstLink, []) << flagRule
                }
            }
        }
    }

    void modify(Map record, Map thing) {
        def obj = thing[sourceLink]
        if (!obj) return

        String flag = obj[flagProperty]
        FlagMatchRule flagRule = flagMatch[flag]
        if (!flagRule) return

        if (flagRule.resourceType && obj[TYPE] == overwriteType) {
            obj[TYPE] = flagRule.resourceType
        }

        if (flagRule.remapProperty) {
            flagRule.remapProperty.each { srcProp, destProp ->
                def src = obj.remove(srcProp)
                if (src) {
                    //if (destProp in obj) ...
                    if (destProp != null) {
                        obj[destProp] = src
                    }
                }
            }
        }
        if (!flagRule.keepFlag) {
            obj.remove(flagProperty)
        }
        if (flagRule.mergeFirstLink) {
            List list = thing[flagRule.mergeFirstLink]
            if (!list) {
                list = thing[flagRule.mergeFirstLink] = []
            }
            boolean mergeOk = false
            def target = list[0]
            if (ld && target) {
                def mappedProperties = flagRule.remapProperty.values()
                boolean targetContainsSomeRemapped = mappedProperties.any {
                    def value = target[it]
                    value && value instanceof String && matchValuePattern.matcher(value)
                }

                boolean onlyExpectedValuesInFuzzyProperty =
                    checkOnlyExpectedValuesInFuzzyProperty(target, obj, mappedProperties)

                if ((!flagRule.revertRequiresNo ||
                        !target.containsKey(flagRule.revertRequiresNo)) &&
                    (targetContainsSomeRemapped ||
                     onlyExpectedValuesInFuzzyProperty)
                   ) {
                    mergeOk = ld.softMerge(obj, target)
                }
            }
            if (!mergeOk) {
                list.add(0, obj)
            }
            thing.remove(sourceLink)
        }
    }

    /**
     * This compares a fuzzy value with stricter values for fuzzy equivalence
     * when merging a node. We do so by removing defined "fuzz" from the fuzzy
     * value, and then remove the stricter values as well. If the result is
     * empty, the fuzzy value is considered equivalent enough.
     */
    boolean checkOnlyExpectedValuesInFuzzyProperty(Map target, Map obj,
            Iterable<String> strictProperties) {
        if (fuzzyMergeProperty) {
            String fuzzyValue = target[fuzzyMergeProperty]
            if (fuzzyValue) {
                fuzzyValue = fuzzPattern.matcher(fuzzyValue).replaceAll('')
                strictProperties.each {
                    String v = obj[it]
                    if (v) {
                        // Same as Pattern.quote, but explicit here since we
                        // toggle parts on again when adding magicValueParts.
                        def vPattern = /\Q${v}\E/
                        magicValueParts?.each { part, pattern ->
                            vPattern = vPattern.replaceAll(part,
                                    Matcher.quoteReplacement(/\E${pattern}\Q/))
                        }
                        fuzzyValue = fuzzyValue.replaceFirst(vPattern, '')
                    }
                }
                return fuzzyValue.trim() == ''
            }
        }
        return false
    }

    void unmodify(Map record, Map thing) {
        flagMatchByLink.each { link, ruleCandidates ->
            def list = thing[link]
            if (!list)
                return

            // TODO: find first obj that matches a flagRule?
            def obj = list[0]
            if (!(obj instanceof Map))
                return

            for (FlagMatchRule flagRule : ruleCandidates) {
                if (flagRule.resourceType != obj[TYPE])
                    continue

                if (flagRule.keepFlag && !obj.containsKey(flagProperty))
                    continue

                if (obj.containsKey(flagProperty) &&
                        obj[flagProperty] != flagRule.flag)
                    continue

                if (flagRule.revertRequiresNo &&
                    obj.containsKey(flagRule.revertRequiresNo))
                    continue

                if (obj[TYPE] != flagRule.resourceType)
                    continue

                if (!flagRule.remapProperty.values().every {
                    if (it == null)
                        return true
                    def value = obj[it]
                    value && value instanceof String && matchValuePattern.matcher(value)
                }) {
                    continue
                }

                obj = [:] + obj
                obj[TYPE] = overwriteType
                obj[flagProperty] = flagRule.flag

                flagRule.remapProperty.each { srcProp, destProp ->
                    if (destProp == null && flagRule.nullValue) {
                        obj[srcProp] = flagRule.nullValue
                    } else {
                        def src = obj.remove(destProp)
                        if (src) {
                            obj[srcProp] = src
                        }
                    }
                }

                thing[sourceLink] = obj
                break
            }
        }
    }

    class FlagMatchRule {
        String flag
        Boolean keepFlag
        String resourceType
        String mergeFirstLink
        Map<String, String> remapProperty
        String revertRequiresNo
        String nullValue
    }

}


class ProduceIfMissingStep extends MarcFramePostProcStepBase {

    String select
    Map produceMissing

    void modify(Map record, Map thing) {
    }

    void unmodify(Map record, Map thing) {
        def source = thing[select]
        def items = source instanceof List ? source : source ? [source] : []
        if (items.any { produceMissing.produceProperty in it }) {
            return
            // Only apply rules in *no* property is found (since if one is
            // found, we don't know if data is intentionally sparse).
        }
        items.each {
            def value = findValue(it, produceMissing.sourcePath)
            if (value) {
                // IMPROVE: use produceMissing.multiple to build from all
                if (value instanceof List) {
                    value = value[0]
                }
                if (produceMissing.showProperties) {
                    value = buildString(value, produceMissing.showProperties)
                }
                if (value) {
                    it[produceMissing.produceProperty] = value
                }
            }
        }
    }
}


class SetFlagsByPatternsStep extends MarcFramePostProcStepBase {

    String select
    List<Map> match
    boolean force = false

    void modify(Map record, Map thing) {
    }

    void unmodify(Map record, Map thing) {
        def selected = thing[select]
        def items = selected instanceof List
                    ? selected : selected ? [selected] : []
        items.each { item ->
            for (rule in match) {
                if (rule.exists.every { item.containsKey(it) }) {
                    rule.setFlags.each { flag, flagValue ->
                        if (!item.containsKey(flag) || force) {
                            item[flag] = flagValue
                        }
                    }
                    break
                }
            }
        }
    }
}

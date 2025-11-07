package whelk.converter.marc

import java.util.regex.Matcher
import java.util.regex.Pattern
import com.fasterxml.jackson.databind.ObjectMapper

import whelk.JsonLd
import whelk.ResourceCache

interface MarcFramePostProcStep {
    var ID = JsonLd.ID_KEY
    var TYPE = JsonLd.TYPE_KEY
    boolean getRequiresResources()
    void setLd(JsonLd ld)
    void setMapper(ObjectMapper mapper)
    void setResourceCache(ResourceCache resourceCache)
    void init()
    void modify(Map record, Map thing)
    void unmodify(Map record, Map thing)
}


abstract class MarcFramePostProcStepBase implements MarcFramePostProcStep {

    String type

    JsonLd ld
    ObjectMapper mapper
    boolean requiresResources = false
    ResourceCache resourceCache

    Pattern matchValuePattern

    void setMatchValuePattern(String pattern) {
        matchValuePattern = Pattern.compile(pattern)
    }

    void init() { }

    def findValue(source, List path) {
        if (!source || !path) {
            return source
        }

        def key = path[0]
        def object
        if (ld == null) {
            object = source[key]
        } else {
            object = getValue(source, key)
        }

        def pathrest = path.size() > 1 ? path[1..-1] : null

        return findValue(object, pathrest)
    }

    def getValue(source, key) {
        if (source instanceof List) {
            if (key instanceof Integer) {
                return source[key]
            }
            return source.findResults { getValue(it, key) }
        }

        if (source instanceof Map) {
            def value = ld.getPropertyValue(source, key)
            if (!value) {
                value = ld.getSubProperties(key).findResult {
                    ld.getPropertyValue(source, (String) it)
                }
            }
            return value
        }

        return source // don't attempt to index potential primitives
    }

    String buildString(Map node, List showProperties) {
        def result = ""
        int shown = 0
        int padded = 0

        for (prop in showProperties) {
            def fmt = null
            if (prop instanceof Map) {
                fmt = prop.useValueFormat
                prop = prop.property
            }
            def value = prop instanceof List ? findValue(node, prop) : getValue(node, prop)
            if (value) {
                shown++
                if (!(value instanceof List)) {
                    value = [value]
                }
                def first = true
                if (fmt?.contentFirst != null) {
                    result += fmt.contentFirst
                }
                def prevAfter = null
                value.each {
                    if (prevAfter != null) {
                        result += prevAfter
                    }
                    if (fmt?.contentBefore != null && (fmt.contentFirst == null || !first)) {
                        result += fmt.contentBefore
                    }
                    result += it
                    first = false
                    prevAfter = fmt?.contentAfter
                }
                if (fmt?.contentLast != null) {
                    result += fmt.contentLast
                } else if (prevAfter != null) {
                    result += prevAfter
                }
            } else if (fmt?.contentNoValue != null) {
                padded++
                result += fmt.contentNoValue
            }
        }

        if (shown == 0 || shown + padded < showProperties.size()) {
            return null
        }

        return result
    }

    Map jsonClone(Map data) {
        return mapper.readValue(mapper.writeValueAsString(data), SortedMap)
    }
}


class CopyOnRevertStep extends MarcFramePostProcStepBase {

    String sourceLink
    String targetLink
    List<FromToProperty> copyIfMissing
    Map injectOnCopies

    void setCopyIfMissing(List<Object> copyIfMissing) {
        this.copyIfMissing = copyIfMissing.collect {
            String fromProp
            String toProp
            if (it instanceof Map) {
                new FromToProperty(it)
            } else if (it instanceof String) {
                new FromToProperty([from: it, to: it])
            } else {
                throw new RuntimeException("Unhandled value in copyIfMissing: ${it}")
            }
        }
    }

    void init() {
        assert sourceLink || targetLink
    }

    void modify(Map record, Map thing) {
    }

    void unmodify(Map record, Map thing) {
        def source = sourceLink ? thing[sourceLink] : thing
        def target = targetLink ? thing[targetLink] : thing

        if (source) {
            if (!(source instanceof List)) {
                source = [source]
            }
            for (prop in copyIfMissing) {
                for (item in source) {
                    if (!target.containsKey(prop.to) && item.containsKey(prop.from)) {
                        def src = item[prop.from]
                        def copy = cloneValue(src)

                        Map inject = prop.injectOnCopies ?: this.injectOnCopies
                        if (inject) {
                            Map injectCopy = jsonClone(inject)
                            if (copy instanceof Map) {
                                injectCopy.keySet().removeAll(copy.keySet())
                                copy.putAll(injectCopy)
                            } else if (copy instanceof List) {
                                copy.each {
                                    if (it instanceof Map) {
                                        injectCopy.keySet().removeAll(it.keySet())
                                        it.putAll(injectCopy)
                                    }
                                }
                            }
                        }
                        target[prop.to] = copy
                    }
                }
            }
        }
    }

    Object cloneValue(Object value) {
        if (value instanceof List) {
            return value.collect { cloneValue(it) }
        } else if (value instanceof Map) {
            return jsonClone((Map) value)
        } else { // assuming String|Integer|Double|Boolean
            return value
        }
    }

    class FromToProperty {
        String from
        String to
        Map injectOnCopies
    }
}


class MappedPropertyStep implements MarcFramePostProcStep {

    String type
    JsonLd ld
    ObjectMapper mapper
    boolean requiresResources = false
    ResourceCache resourceCache = null

    String sourceEntity
    String sourceLink
    String sourceProperty
    String targetEntity
    String targetProperty
    Map<String, Object> valueMap

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
        if (sourceLink) {
            source = source.get(sourceLink)
        }
        def values = source?.get(sourceProperty)
        if (values instanceof String) {
            values = [values]
        }

        for (value in values) {
            def mapped = valueMap[value]
            if (mapped != null) {
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
            def linked = thing[flagRule.mergeFirstLink]
            List list = linked instanceof List ? linked : linked ? [linked] : null
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
                strictProperties.each {
                    String value = obj[it]
                    if (value) {
                        fuzzyValue = removeMatchingValue(fuzzyValue, value,
                                magicValueParts)
                    }
                }
                fuzzyValue = fuzzPattern.matcher(fuzzyValue).replaceAll('')
                fuzzyValue = fuzzyValue.trim()
                return fuzzyValue == ''
            }
        }
        return false
    }

    /**
     * @param fuzzyValue The value from which to remove other values.
     * @param value A value to remove.
     * @param magicValueParts A map of tokens in the value which are to be
     * treated as magic, represented by regular expressions.
     */
    private String removeMatchingValue(String fuzzyValue, String value,
            Map<String, String> magicValueParts) {
        // Same as Pattern.quote, but explicit here since we
        // toggle parts on again when adding magicValueParts.
        def vPattern = /\Q${value}\E/
        magicValueParts?.each { part, pattern ->
            vPattern = vPattern.replaceAll(part,
                    Matcher.quoteReplacement(/\E${pattern}\Q/))
        }
        fuzzyValue = fuzzyValue.replaceFirst(vPattern, '')
        return fuzzyValue
    }

    void unmodify(Map record, Map thing) {
        flagMatchByLink.each { link, ruleCandidates ->
            def list = thing[link]
            if (!list)
                return

            // TODO: find first obj that matches a flagRule?
            def obj = list instanceof List ? list[0] : list
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


class InjectWhenMatchingOnRevertStep extends MarcFramePostProcStepBase {

    List<Map> rules
    static final MATCH_REF = ~/\?\d+/

    void modify(Map record, Map thing) {
    }

    void unmodify(Map record, Map thing) {
        def item = thing
        for (rule in rules) {
            def refs = [:]
            if (deepMatches(item, rule.matches, refs)) {
                Map injectData = jsonClone(rule.injectData)
                injectInto(item, injectData, refs)
                break
            }
        }
    }

    boolean deepMatches(Map o, Map match, Map refs) {
        String ref = null
        if (match[ID] ==~ MATCH_REF) {
            ref = match[ID]
        }
        match.every { k, v ->
            if (k == ID && v == ref) return true
            Util.asList(v).every {
                return Util.asList(o[k]).any { ov ->
                    boolean matches = (ov instanceof Map) ?
                        deepMatches(ov, it, refs) :
                        ov == it
                    if (!matches && k == TYPE) {
                        matches = ld?.isSubClassOf(ov, it)
                    }
                    if (matches) {
                        refs[ref] = o
                    }
                    return matches
                }
            }
        }
    }

    static void injectInto(Map receiver, Map v, Map refs) {
        v.each { ik, iv ->
            if (receiver.containsKey(ik)) {
                if (iv instanceof Map && iv[ID] ==~ MATCH_REF) {
                    Map matched = refs[iv[ID]]
                    if (matched instanceof Map && !iv.keySet().any {
                        it != ID && matched.containsKey(it)
                    }) {
                        matched.putAll(iv)
                        matched.remove(ID)
                    }
                } else if (receiver[ik] != iv) {
                    def values = Util.asList(receiver[ik])
                    def ids = values.findResults { if (it instanceof Map) it[ID] } as Set
                    Util.asList(iv).each {
                        if (it instanceof Map && it[ID] in ids)
                            return
                        values << it
                    }
                    receiver[ik] = values
                }
            } else {
                receiver[ik] = iv
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
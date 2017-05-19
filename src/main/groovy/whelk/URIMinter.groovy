package whelk

import whelk.util.URIWrapper

import java.util.zip.CRC32

import com.damnhandy.uri.template.UriTemplate

import groovy.util.logging.Log4j2 as Log


@Log
class URIMinter extends IdGenerator {
    URIWrapper base = new URIWrapper("/")
    String typeKey = '@type'
    String documentUriTemplate
    String thingUriTemplate
    String objectLink
    String epochDate
    String randomVariable = null
    int maxRandom = 0
    String timestampVariable = null
    boolean timestampCaesarCipher = false
    String uuidVariable = null
    boolean slugCharInAlphabet = true
    String slugCase = "lower"
    String compoundSlugSeparator = ""
    Map<String, MintRuleSet> rulesByDataset
    int minSlugSize = 2

    private long epochOffset

    URIMinter() {
        this.timestampCaesarCipher = timestampCaesarCipher
        this.setEpochDate(epochDate)
        this.alphabet = alphabet
    }

    void setBase(String uri) {
        this.base = new URIWrapper(uri)
    }

    void setEpochDate(String epochDate) {
        if (epochDate instanceof String) {
            epochOffset = DateUtil.parseDate(epochDate).getTime()
        }
        this.epochDate = epochDate
    }

    void setRulesByDataset(Map rules) {
        def map = [:]
        this.rulesByDataset = map
        rules.each { k, v ->
            map[k] = new MintRuleSet(v)
        }
    }

    int createRandom() {
        return new Random().nextInt(maxRandom)
    }

    long createTimestamp() {
        return System.currentTimeMillis() - epochOffset
    }

    String createUUID() {
        return UUID.randomUUID()
    }

    URIWrapper mint(Document doc) {
        return base.resolve(computePath(doc))
    }


    String computePath(Document doc) {
        computePath(doc.data)
    }

    Map computePaths(Map data, String collection) {
        def results = [:]
        def object = data
        if (objectLink) {
            object = data[objectLink]
        }
        if (documentUriTemplate) {
            def thingUri = computePath(object, collection)
            results['thing'] = thingUri
            results['document'] = UriTemplate.expand(documentUriTemplate,
                    [thing: thingUri])
        } else {
            def documentUri = computePath(object, collection)
            results['document'] = documentUri
            if (thingUriTemplate) {
                results['thing'] = UriTemplate.expand(thingUriTemplate,
                        [document: documentUri])
            }
        }
        log.debug "Computed ${results} for object in ${collection}"
        return results
    }

    String computePath(Map data, String collection) {
        def vars = [:]
        if (timestampVariable) {
            vars[timestampVariable] = baseEncode(createTimestamp(), timestampCaesarCipher)
        }
        if (randomVariable) {
            vars[randomVariable] = baseEncode(createRandom())
        }
        if (uuidVariable) {
            vars[uuidVariable] = createUUID()
        }

        def type = data[typeKey]
        if (type instanceof List) {
            type = type[0]
        }
        def ruleset = rulesByDataset[collection]
        def rule = ruleset.ruleByType[type] ?: ruleset.ruleByType['*']

        vars['basePath'] = rule.basePath

        if (rule.variables) {
            rule.variables.each {
                def obj = data
                def prop = it
                def dotIndex = it.indexOf('.')
                if (dotIndex != -1) {
                    obj = data[it.substring(0, dotIndex)]
                    prop = it.substring(dotIndex)
                }
                def slug = obj[prop]
                if (!slug) {
                    throw new URIMinterException("Missing value for variable ${it}")
                }
                vars[it] = slug
            }
        }

        if (rule.compoundSlugFrom) {
            def compundKey = collectCompundKey(rule.compoundSlugFrom, data)
            if (compundKey.size()) {
                def slug = compundKey.collect { scramble(it) }.join(compoundSlugSeparator)
                vars['compoundSlug'] = slug
            }
        }

        def uriTemplate = rule.uriTemplate ?: ruleset.uriTemplate
        return UriTemplate.expand(uriTemplate, vars)
    }

    List collectCompundKey(keys, data, compound=[], pickFirst=false) {
        for (key in keys) {
            if (key instanceof List) {
                def subCompound = collectCompundKey(key, data, [], !pickFirst)
                if (subCompound.size()) {
                    compound.addAll(subCompound)
                    if (pickFirst) {
                        return compound
                    }
                }
            } else if (key instanceof Map) {
                key.each { subKey, valueKeys ->
                    def subData = data[subKey]
                    if (subData) {
                        collectCompundKey(valueKeys, subData, compound)
                    }
                }
            } else {
                def value = data[key]
                if (value) {
                    compound << value
                    if (pickFirst) {
                        return compound
                    }
                }
            }
        }
        return compound
    }

    String scramble(String s) {
        if (slugCase == "lower") {
            s = s.toLowerCase()
        }
        if (slugCharInAlphabet) {
            def reduced = s.findAll { alphabet.contains(it) }.join("")
            if (reduced.size() >= minSlugSize) {
                return reduced
            }
        }
        return s
    }

    static class MintRuleSet {
        Map<String, MintRule> ruleByType = [:]
        String uriTemplate
        MintRuleSet(Map rules) {
            this.uriTemplate = rules.uriTemplate
            rules.ruleByBaseType.each { String type, Map ruleCfg ->
                def rule = new MintRule(ruleCfg)
                if (!rule.uriTemplate) {
                    rule.uriTemplate = this.uriTemplate
                }
                ruleByType[type] = rule
                ruleCfg.subclasses.each {
                    ruleByType[it] = rule
                }
            }
        }
    }

    static class MintRule {
        Map<String, String> bound
        List<String> variables
        List subclasses
        List compoundSlugFrom
        String uriTemplate
        String basePath
    }

}

class URIMinterException extends RuntimeException {
    URIMinterException(String msg) {
        super(msg)
    }
}

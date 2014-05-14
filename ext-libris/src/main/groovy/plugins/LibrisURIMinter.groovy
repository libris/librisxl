package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import com.damnhandy.uri.template.UriTemplate

import se.kb.libris.whelks.Document


@Log
class LibrisURIMinter extends BasicPlugin implements URIMinter {

    static final char[] ALPHANUM = "0123456789abcdefghijklmnopqrstuvwxyz".chars
    static final char[] VOWELS = "auoeiy".chars
    static final char[] DEVOWELLED = ALPHANUM.findAll { !VOWELS.contains(it) } as char[]

    URI base
    String typeKey = '@type'
    String documentUriTemplate
    String documentThingLink
    String thingUriTemplate
    String thingDocumentLink
    String epochDate
    char[] alphabet = ALPHANUM
    String randomVariable = null
    int maxRandom = 0
    String timestampVariable = null
    boolean timestampCaesarCipher = false
    String uuidVariable = null
    String compoundSlugSeparator = ""
    Map<String, MintRuleSet> rulesByDataset
    int minKeySize = 3

    private long epochOffset

    String pathSep = "/"
    String partSep = "-"
    String keySep = ""

    LibrisURIMinter() {
        if (base != null) {
            this.setBase(base)
        }
        this.timestampCaesarCipher = timestampCaesarCipher
        this.setEpochDate(epochDate)
        this.alphabet = alphabet
    }

    void setBase(String uri) {
        this.base = new URI(uri)
    }

    void setEpochDate(String epochDate) {
        if (epochDate instanceof String) {
            epochOffset = Date.parse("yyyy-MM-dd", epochDate).getTime()
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

    URI mint(Document doc) {
        return base.resolve(computePath(doc))
    }

    String computePath(Document doc) {
        computePath(doc.getDataAsMap()) // TODO: needs JSON-aware doc
    }

    String computePath(Map data, String dataset) {
        def document = null
        def thing = null
        if (documentThingLink) {
            document = data
            thing = data[documentThingLink]
        } else if (thingDocumentLink) {
            thing = data
            document = data[thingDocumentLink]
        } else {
            document = data
        }

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

        def type = thing[typeKey]
        if (type instanceof List) {
            type = type[0]
        }
        def ruleset = rulesByDataset[dataset]
        def rule = ruleset.ruleByType[type] ?: ruleset.ruleByType['*']

        vars['basePath'] = rule.basePath

        def compundKey = collectCompundKey(rule.compoundSlugFrom, thing)
        if (compundKey.size()) {
            def slug = compundKey.collect { scramble(it) }.join(compoundSlugSeparator)
            vars['compoundSlug'] = slug
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
        //if (slugCase == "lower")
        def ls = s.toLowerCase()
        //if (slugCharInAlphabet)
        def rs = ls.findAll { alphabet.contains(it) }.join("")
        return (rs.size() < minKeySize)? ls : rs
    }

    String baseEncode(long n, boolean lastDigitBasedCaesarCipher=false) {
        int base = alphabet.length
        int[] positions = basePositions(n, base)
        if (lastDigitBasedCaesarCipher) {
            int rotation = positions[-1]
            for (int i=0; i < positions.length - 1; i++) {
                positions[i] = rotate(positions[i], rotation, base)
            }
        }
        return baseEncode((int[]) positions)
    }

    String baseEncode(int[] positions) {
        def chars = positions.collect { alphabet[it] }
        return chars.join("")
    }

    int[] basePositions(long n, int base) {
        int maxExp = Math.floor(Math.log(n) / Math.log(base))
        int[] positions = new int[maxExp + 1]
        for (int i=maxExp; i > -1; i--) {
            positions[maxExp-i] = (int) (((n / (base ** i)) as long) % base)
        }
        return positions
    }

    int rotate(int i, int rotation, int ceil) {
        int j = i + rotation
        return (j >= ceil)? j - ceil : j
    }

    class MintRuleSet {
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

    class MintRule {
        Map<String, String> variables
        List subclasses
        List compoundSlugFrom
        String uriTemplate
        String basePath
    }

}

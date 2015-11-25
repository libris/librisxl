package whelk.converter

import groovy.util.logging.Slf4j as Log

import com.damnhandy.uri.template.UriTemplate

import whelk.Document

import java.util.zip.CRC32


@Log
class URIMinter {

    static final char[] ALPHANUM = "0123456789abcdefghijklmnopqrstuvwxyz".chars
    static final char[] VOWELS = "auoeiy".chars
    static final char[] DEVOWELLED = ALPHANUM.findAll { !VOWELS.contains(it) } as char[]

    static final Map<String,Long> BASETIMES = [
            "auth": Date.parse("yyyy-MM-dd", "1999-01-01").getTime(),
             "bib": Date.parse("yyyy-MM-dd", "2000-01-01").getTime(),
            "hold": Date.parse("yyyy-MM-dd", "2001-01-01").getTime()
    ]

    static final int IDENTIFIER_LENGTH = 13

    URI base = new URI("/")
    String typeKey = '@type'
    String documentUriTemplate
    String thingUriTemplate
    String objectLink
    String epochDate
    static char[] alphabet = DEVOWELLED
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
    int maxWordsInSlug = 6
    int shortWordSize = 3

    private long epochOffset
    private int checkForWordsMinSize = maxWordsInSlug * (shortWordSize + 1)

    URIMinter() {
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

    static String mint(String originalIdentifier, URI base = new URI("/")) {
        String[] parts = originalIdentifier.split("/")
        String dataset = parts[1]
        int numericId = Integer.parseInt(parts.last())
        return mint(BASETIMES.get(dataset)+numericId, originalIdentifier, base, 12)
    }

    static String mint(long timestamp, String seed = null, URI base = new URI("/"), idLength = IDENTIFIER_LENGTH) {
        StringBuilder identifier = new StringBuilder(baseEncode(timestamp, true))
        if (seed) {
            CRC32 crc32 = new CRC32()
            crc32.update(seed.getBytes("UTF-8"))
            identifier.append(baseEncode(crc32.value, false))
            if (identifier.length() > idLength) {
                identifier = new StringBuilder(identifier.substring(0, idLength))
            }
        } else {
            while (identifier.length() < idLength) {
                identifier.append(DEVOWELLED[new Random().nextInt(DEVOWELLED.length)])
            }
        }
        return base.resolve(identifier.toString()).toString()
    }

    String computePath(Document doc) {
        computePath(doc.data)
    }

    Map computePaths(Map data, String dataset) {
        def results = [:]
        def object = data
        if (objectLink) {
            object = data[objectLink]
        }
        if (documentUriTemplate) {
            def thingUri = computePath(object, dataset)
            results['thing'] = thingUri
            results['document'] = UriTemplate.expand(documentUriTemplate,
                    [thing: thingUri])
        } else {
            def documentUri = computePath(object, dataset)
            results['document'] = documentUri
            if (thingUriTemplate) {
                results['thing'] = UriTemplate.expand(thingUriTemplate,
                        [document: documentUri])
            }
        }
        log.debug "Computed ${results} for object in ${dataset}"
        return results
    }

    String computePath(Map data, String dataset) {
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
        def ruleset = rulesByDataset[dataset]
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
                    compound << shorten(value)
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

    String shorten(String s) {
        if (maxWordsInSlug && s.size() > checkForWordsMinSize) {
            def words = s.split(/\s+|-/)
            if (words.size() > maxWordsInSlug) {
                return words.collect {
                    it.size() > shortWordSize? it.substring(0, shortWordSize) : it
                }.join(" ")
            }
        }
        return s
    }

   static String baseEncode(long n, boolean lastDigitBasedCaesarCipher=false) {
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

    static String baseEncode(int[] positions) {
        def chars = positions.collect { alphabet[it] }
        return chars.join("")
    }

    static int[] basePositions(long n, int base) {
        int maxExp = Math.floor(Math.log(n) / Math.log(base))
        int[] positions = new int[maxExp + 1]
        for (int i=maxExp; i > -1; i--) {
            positions[maxExp-i] = (int) (((n / (base ** i)) as long) % base)
        }
        return positions
    }

    static int rotate(int i, int rotation, int ceil) {
        int j = i + rotation
        return (j >= ceil)? j - ceil : j
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

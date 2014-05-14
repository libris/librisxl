package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

@Log
class LibrisURIMinter extends BasicPlugin implements URIMinter {

    static final char[] ALPHANUM = "0123456789abcdefghijklmnopqrstuvwxyz".chars
    static final char[] VOWELS = "auoeiy".chars
    static final char[] DEVOWELLED = ALPHANUM.findAll { !VOWELS.contains(it) } as char[]

    String originDate
    URI base
    Map typeRules
    String pathSep = "/"
    String partSep = "-"
    String keySep = ""
    char[] alphabet
    int minKeySize = 3
    int randWidth = 0
    boolean caesarCiphered = false

    private long epochOffset
    private int randCeil

    LibrisURIMinter(base=null, typeRules=null, originDate=null, caesarCiphered=false, alphabet=DEVOWELLED) {
        if (base != null) {
            this.setBase(base)
        }
        this.caesarCiphered = caesarCiphered
        this.setOriginDate(originDate)
        this.typeRules = typeRules
        this.alphabet = alphabet
        this.setRandWidth(0)
    }

    void setBase(String uri) {
        this.base = new URI(uri)
    }

    void setOriginDate(String originDate) {
        if (originDate instanceof String) {
            epochOffset = Date.parse("yyyy-MM-dd", originDate).getTime()
        }
        this.originDate = originDate
    }

    void setRandWidth(int randWidth) {
        this.randCeil = alphabet.length ** randWidth
        this.randWidth = randWidth
    }

    URI mint(Document doc, boolean remint=true) {
        if (!remint && doc.identifier) {
            return new URI(doc.identifier)
        }

        if (typeRules) {
            return base.resolve(computePath(doc))
        } else {
            return base.resolve("/uuid/"+ UUID.randomUUID()) // urn:uuid:...
        }
    }

    String computePath(Document doc) {
        computePath(doc.getDataAsMap()) // TODO: needs JSON-aware doc
    }

    String computePath(Map data) {
        def keys = []
        collectKeys(typeRules, data, keys)
        def type = keys[0]
        keys = keys[1..-1]

        def codes = []
        if (originDate) {
            codes << getTimestamp()
        }
        if (randWidth) {
            codes << new Random().nextInt(randCeil)
        }

        return makePath(type, codes, keys)
    }


    void collectKeys(rules, data, keys) {
        // TODO: order of rules
        rules.each { key, rule ->
            def v = data[key]
            if (rule.is(true)) {
                keys << v
            } else if (rule instanceof Map) {
                collectKeys(rule, v, keys)
            }
        }
    }

    long getTimestamp() {
        return System.currentTimeMillis() - epochOffset
    }

    String makePath(String type, List<Long> codes, List<String> keys) {
        def typeSegment = segmentFor(type)
        def parts = codes.collect { baseEncode(it) }
        def keyRepr = keys.collect { scramble(it) }.join(keySep)
        if (keyRepr) {
            parts << keyRepr
        }
        def reprs = []
        if (typeSegment) {
            reprs << typeSegment
        }
        def partRepr = parts.join(partSep)
        reprs << partRepr
        return reprs.join(pathSep)
    }

    String segmentFor(String name) {
        return name.toLowerCase()
    }

    String baseEncode(long n) {
        baseEncode(n, caesarCiphered)
    }

    String baseEncode(long n, boolean caesarCiphered) {
        int base = alphabet.length
        int[] positions = basePositions(n, base)
        if (caesarCiphered) {
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

    String scramble(String s) {
        def ls = s.toLowerCase()
        def rs = ls.findAll { alphabet.contains(it) }.join("")
        return (rs.size() < minKeySize)? ls : rs
    }

}

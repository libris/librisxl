package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

@Log
class LibrisMinter extends BasicPlugin implements URIMinter {

    static final char[] ALPHANUM
    static final char[] VOWELS
    static final char[] DEVOWELLED

    static {
        ALPHANUM = "0123456789abcdefghijklmnopqrstuvwxyz".chars
        VOWELS = "auoeiy".chars
        DEVOWELLED = ALPHANUM.findAll { !VOWELS.contains(it) } as char[]
    }

    String originDate
    URI baseUri
    Map typeRules
    String pathSep = "/"
    String partSep = "-"
    String keySep = ""
    char[] alphabet
    int minSize = 3
    int randWidth = 5

    private long epochOffset
    private int randCeil

    LibrisMinter(baseUri=null, originDate=null, typeRules=null, alphabet=DEVOWELLED) {
        this.baseUri = (baseUri instanceof URI)? baseUri : new URI(baseUri.toString())
        this.originDate = originDate
        if (originDate instanceof String) {
            epochOffset = Date.parse("yyyy-MM-dd", originDate).getTime()
        }
        this.typeRules = typeRules
        this.alphabet = alphabet
        this.randCeil = alphabet.length * 5
    }

    URI mint(Document doc, boolean remint=true) {
        if (!remint && doc.identifier) {
            return new URI(doc.identifier)
        }

        if (typeRules)
            return baseUri.resolve(computePath(doc))
        else
            return baseUri.resolve("/uuid/"+ UUID.randomUUID()) // urn:uuid:...
    }

    String computePath(Document doc) {
        computePath(doc.getData())
    }

    String computePath(Map data) {
        // TODO: use rules to get these from data
        String primaryType = "Thing"
        def keys = []
        def codes = []
        if (originDate) {
            codes << getTimestamp()
        }
        if (randWidth) {
            codes << new Random().nextInt(randCeil)
        }
        return makePath(type, codes, keys)
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
        int base = alphabet.length
        int width = Math.floor(Math.log(n) / Math.log(base))
        def chars = []
        for (int i=width; i > -1; i--) {
            chars << alphabet[((n / (base ** i)) as long) % base]
        }
        return chars.join("")
    }

    String scramble(String s) {
        def ls = s.toLowerCase()
        def rs = ls.findAll { alphabet.contains(it) }.join("")
        return (rs.size() < minSize)? ls : rs
    }

}

package whelk.util

/**
 * A wrapper around the URI class which (unlike the original) tolerates the "excluded ASCII chars"
 * This class does _not_ inherit from URI because we _want_ compilation faults when using the wrong one (URI).
 */
public class URIWrapper {

    /* List of excluded ASCII chars per https://www.ietf.org/rfc/rfc2396.txt */
    private static final Map percentEncoding =
            [
                " ": "%20",
                "<": "%3C",
                ">": "%3E",
                "#": "%23",
                "%": "%25",
                "\"": "%22",
                "{": "%7B",
                "}": "%7D",
                "|": "%7C",
                "\\": "%5C",
                "^": "%5E",
                "[": "%5B",
                "]": "%5D",
                "`": "%60"
            ]

    private static final Map percentDecoding = [:]
    static {
        for (String key : percentEncoding.keySet()) {
            String value = percentEncoding.get(key)
            percentDecoding.put(value, key)
        }
    }

    private final URI m_internalUri
    private final boolean m_percentEncoded

    private static String percentEncode(String string) {
        return translate(string, percentEncoding)
    }

    private static String percentDecode(String string) {
        return translate(string, percentDecoding)
    }

    private static String translate(String string, Map substitutionTable) {
        for (String key : substitutionTable.keySet()) {
            string = string.replace(key, (String) substitutionTable.get(key))
        }
        return string
    }

    private String getDecoded(String string) {
        if (m_percentEncoded)
            return percentDecode(string)
        else
            return string
    }

    public URIWrapper(URI uri) {
        m_internalUri = uri
        m_percentEncoded = false
    }

    public URIWrapper(String uriString) {
        try {
            m_internalUri = URI.create(uriString)
            m_percentEncoded = false
        } catch (IllegalArgumentException iae) {
            String encoded = percentEncode(uriString)
            m_internalUri = URI.create(encoded)
            m_percentEncoded = true
        }
    }

    public URIWrapper resolve(URIWrapper uri) {
        return new URIWrapper(m_internalUri.resolve(uri.m_internalUri))
    }

    public URIWrapper resolve(String uriString) {
        try {
            return new URIWrapper(m_internalUri.resolve(uriString))
        } catch (IllegalArgumentException iae) {
            URIWrapper wrapper = new URIWrapper(uriString)
            return resolve(wrapper)
        }
    }

    public String toString() { return getDecoded( URLDecoder.decode(m_internalUri.toString(), "utf-8")) }
    public String getPath() { return getDecoded(m_internalUri.getPath()) }
    public String getAuthority() { return getDecoded(m_internalUri.getAuthority()) }
    public String getFragment() { return getDecoded(m_internalUri.getFragment()) }
    public String getHost() { return getDecoded(m_internalUri.getHost()) }
    public String getQuery() { return getDecoded(m_internalUri.getQuery()) }
    public String getScheme() { return getDecoded(m_internalUri.getScheme()) }
    public String getRawAuthority() { return getDecoded(m_internalUri.getRawAuthority()) }
    public String getRawFragment() { return getDecoded(m_internalUri.getRawFragment()) }
    public String getRawPath() { return getDecoded(m_internalUri.getRawPath()) }
    public String getRawQuery() { return getDecoded(m_internalUri.getRawQuery()) }
}

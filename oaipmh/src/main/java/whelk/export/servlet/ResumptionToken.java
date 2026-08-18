package whelk.export.servlet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static whelk.util.Jackson.mapper;

/**
 * Stateless OAI-PMH resumptionToken.
 *
 * All state needed to resume a ListRecords/ListIdentifiers harvest is encoded into the Base64-encoded
 * token itself.
 */
public class ResumptionToken
{
    // Original request parameters
    public final String from;            // ISO8601 or null
    public final String until;           // ISO8601, never null (frozen to now() on the first request)
    public final String set;             // setSpec or null
    public final String metadataPrefix;  // never null
    public final boolean withDeletedData;
    public final boolean withSilentChanges;

    // Keyset cursor: (modified, id) of the last source row consumed from lddb
    public final String afterModified;   // ISO8601 timestamp string, or null before the first row
    public final String afterId;         // lddb.id, or null before the first row

    public ResumptionToken(String from, String until, String set, String metadataPrefix,
                           boolean withDeletedData, boolean withSilentChanges,
                           String afterModified, String afterId) {
        this.from = from;
        this.until = until;
        this.set = set;
        this.metadataPrefix = metadataPrefix;
        this.withDeletedData = withDeletedData;
        this.withSilentChanges = withSilentChanges;
        this.afterModified = afterModified;
        this.afterId = afterId;
    }

    public static ResumptionToken parse(String token) {
        try {
            byte[] decoded = Base64.getUrlDecoder().decode(token);
            @SuppressWarnings("unchecked")
            Map<String, Object> m = mapper.readValue(new String(decoded, StandardCharsets.UTF_8), HashMap.class);

            // until and metadataPrefix are mandatory in any valid token.
            if (m.get("u") == null || m.get("p") == null)
                return null;

            return new ResumptionToken(
                    (String) m.get("f"),
                    (String) m.get("u"),
                    (String) m.get("s"),
                    (String) m.get("p"),
                    Boolean.TRUE.equals(m.get("d")),
                    Boolean.TRUE.equals(m.get("c")),
                    (String) m.get("am"),
                    (String) m.get("ai"));
        } catch (IllegalArgumentException | IOException e) {
            return null;
        }
    }

    public String toToken() {
        Map<String, Object> m = new HashMap<>();
        m.put("u", until);
        m.put("p", metadataPrefix);
        if (from != null) m.put("f", from);
        if (set != null) m.put("s", set);
        if (withDeletedData) m.put("d", true);
        if (withSilentChanges) m.put("c", true);
        if (afterModified != null) m.put("am", afterModified);
        if (afterId != null) m.put("ai", afterId);

        try {
            byte[] json = mapper.writeValueAsBytes(m);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ResumptionToken withCursor(String newAfterModified, String newAfterId) {
        return new ResumptionToken(from, until, set, metadataPrefix, withDeletedData, withSilentChanges,
                newAfterModified, newAfterId);
    }
}

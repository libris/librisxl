package whelk.export.servlet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ResumptionTokenTest
{
    @Test
    public void roundTripsAllFields()
    {
        ResumptionToken token = new ResumptionToken(
                "2020-01-01T00:00:00Z", "2026-06-16T10:00:00Z", "bib:S", "marcxml",
                true, true, "2021-03-04T08:00:00.123Z", "abcd1234");

        ResumptionToken parsed = ResumptionToken.parse(token.toToken());

        assertNotNull(parsed);
        assertEquals("2020-01-01T00:00:00Z", parsed.from);
        assertEquals("2026-06-16T10:00:00Z", parsed.until);
        assertEquals("bib:S", parsed.set);
        assertEquals("marcxml", parsed.metadataPrefix);
        assertTrue(parsed.withDeletedData);
        assertTrue(parsed.withSilentChanges);
        assertEquals("2021-03-04T08:00:00.123Z", parsed.afterModified);
        assertEquals("abcd1234", parsed.afterId);
    }

    @Test
    public void roundTripsMinimalFields()
    {
        // No from/set/flags, no cursor yet (as on the very first page).
        ResumptionToken token = new ResumptionToken(
                null, "2026-06-16T10:00:00Z", null, "oai_dc", false, false, null, null);

        ResumptionToken parsed = ResumptionToken.parse(token.toToken());

        assertNotNull(parsed);
        assertNull(parsed.from);
        assertEquals("2026-06-16T10:00:00Z", parsed.until);
        assertNull(parsed.set);
        assertEquals("oai_dc", parsed.metadataPrefix);
        assertFalse(parsed.withDeletedData);
        assertFalse(parsed.withSilentChanges);
        assertNull(parsed.afterModified);
        assertNull(parsed.afterId);
    }

    @Test
    public void withCursorPreservesRequestAndAdvancesPosition()
    {
        ResumptionToken base = new ResumptionToken(
                "2020-01-01T00:00:00Z", "2026-06-16T10:00:00Z", "auth", "rdfxml", false, false, null, null);

        ResumptionToken advanced = base.withCursor("2022-05-05T00:00:00Z", "xyz");

        assertEquals(base.from, advanced.from);
        assertEquals(base.until, advanced.until);
        assertEquals(base.set, advanced.set);
        assertEquals(base.metadataPrefix, advanced.metadataPrefix);
        assertEquals("2022-05-05T00:00:00Z", advanced.afterModified);
        assertEquals("xyz", advanced.afterId);
    }

    @Test
    public void parseRejectsGarbage()
    {
        assertNull(ResumptionToken.parse("not-a-valid-token!!!"));
        assertNull(ResumptionToken.parse(""));
    }

    @Test
    public void parseRejectsTokenMissingMandatoryFields()
    {
        // Valid Base64URL JSON, but missing the mandatory 'until' (u) and 'metadataPrefix' (p).
        String json = "{\"f\":\"2020-01-01T00:00:00Z\"}";
        String encoded = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertNull(ResumptionToken.parse(encoded));
    }

    @Test
    public void tokenIsUrlSafe()
    {
        // A token is handed back to harvesters verbatim in a URL; it must not contain characters
        // that would need further escaping.
        ResumptionToken token = new ResumptionToken(
                null, "2026-06-16T10:00:00Z", "hold:Some/Sigel+Weird", "marcxml_expanded",
                false, false, "2021-03-04T08:00:00.123Z", "id/with+chars");

        String encoded = token.toToken();
        assertFalse(encoded.contains("+"));
        assertFalse(encoded.contains("/"));
        assertFalse(encoded.contains("="));

        ResumptionToken parsed = ResumptionToken.parse(encoded);
        assertNotNull(parsed);
        assertEquals("hold:Some/Sigel+Weird", parsed.set);
        assertEquals("id/with+chars", parsed.afterId);
    }
}

package se.kb.libris.digi;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import whelk.Configuration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static se.kb.libris.digi.DigitalReproductionAPI.Type.ARRAY;
import static se.kb.libris.digi.DigitalReproductionAPI.Type.STRING;
import static se.kb.libris.digi.RequestException.badRequest;
import static se.kb.libris.digi.Util.JSONLD;
import static se.kb.libris.digi.Util.getAtPath;
import static se.kb.libris.digi.Util.isLink;
import static se.kb.libris.digi.Util.isTruthy;

// TODO clean up digital vs electronic when type normalization has landed in production

/**
 Creates a record for a digital reproduction.
 Takes JSON-LD with a DigitalResource/Electronic describing the reproduction as input.

 - Validates that minimal required data is present in DigitalResource/Electronic (all additional data is kept).
 - Extracts and links work entity from reproduction and original (physical thing)
 - Copies title from original
 - Adds DIGI and DST bibliographies if applicable
 - Adds carrierType rda/OnlineResource if applicable
 - Creates holdings if specified in @reverse.itemOf
 - Record data can be specified in meta


Example:

TOKEN=$(curl -s -X POST -d 'client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&grant_type=client_credentials' https://login-dev.libris.kb.se/oauth/token | jq -r .access_token)

curl -v -XPOST 'http://localhost:8180/_reproduction' -H 'Content-Type: application/ld+json' -H "Authorization: Bearer $TOKEN" -H 'XL-Active-Sigel: S' --data-binary @- << EOF
{
  "@type": "Electronic",
  "reproductionOf": { "@id": "http://libris.kb.se.localhost:5000/q822pht24j3ljjr#it" },
  "production": [
    {
      "@type": "Reproduction",
      "agent": { "@id": "http://libris.kb.se.localhost:5000/jgvxv7m23l9rxd3#it" },
      "place": { "@type": "Place", "label": "Stockholm" },
      "date": "2021"
    }
  ],
  "meta" : {
    "bibliography": [ {"@id" : "https://libris.kb.se/library/ARB"} ]
  },
  "@reverse" : {
    "itemOf": [
      {
        "heldBy": { "@id": "https://libris.kb.se/library/S" },
        "hasComponent": [{ "cataloguersNote": ["foo"] }]
      },
      {
        "heldBy": { "@id": "https://libris.kb.se/library/Utb1" },
        "cataloguersNote": ["bar"],
        "meta": { "cataloguersNote": ["baz"] }
      }
    ]
  }
}
EOF

 */
public class DigitalReproductionAPI extends HttpServlet {
    private static final Logger log = LoggerFactory.getLogger(DigitalReproductionAPI.class);

    static final String API_LOCATION = "https://libris.kb.se/api/_reproduction"; // Only for setting generationProcess

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final Set<String> FORWARD_HEADERS = Set.of(
            "xl-active-sigel",
            "authorization"
    );

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Map<String, String> forwardHeaders = new LinkedHashMap<>();
        for (String name : Collections.list(request.getHeaderNames())) {
            if (FORWARD_HEADERS.contains(name.toLowerCase())) {
                forwardHeaders.put(name, request.getHeader(name));
            }
        }

        var service = new ReproductionService(new XL(forwardHeaders, getXlAPI()));

        try {
            boolean extractWork = !Boolean.parseBoolean(request.getParameter("dont-extract-work"));
            String id = service.createDigitalReproduction(parse(request), extractWork);
            log.info("Created {}", id);
            response.setHeader("Location", id);
            response.setStatus(SC_CREATED);
        } catch (RequestException e) {
            log.warn("{} {}", e.code, e.msg);
            sendError(response, e.code, e.msg);
        } catch (Exception e) {
            log.error("Internal error: " + e, e);
            sendError(response, SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    static Map<String, Object> parse(HttpServletRequest request) throws IOException {
        if (!JSONLD.equals(request.getHeader("Content-Type"))) {
            throw badRequest("Header Content-Type must be " + JSONLD);
        }

        Map<String, Object> electronic = readJson(request);

        try {
            check(electronic, List.of("@type"), "Electronic");
        } catch (RequestException ignored) {
            check(electronic, List.of("@type"), "DigitalResource");
        }

        check(electronic, List.of("reproductionOf", "@id"), STRING);
        check(electronic, List.of("production"), ARRAY);
        if (!isLink(getAtPath(electronic, List.of("production", 0)))) { // minimal valid shape, so just check the first one
            check(electronic, List.of("production", 0, "@type"), "Reproduction");
            check(electronic, List.of("production", 0, "date"), STRING);
            if (!isLink(getAtPath(electronic, List.of("production", 0, "agent")))) {
                check(electronic, List.of("production", 0, "place", "@type"), STRING);
            }
            if (!isLink(getAtPath(electronic, List.of("production", 0, "place")))) {
                check(electronic, List.of("production", 0, "place", "@type"), "Place");
                check(electronic, List.of("production", 0, "place", "label"), STRING);
            }
        }

        return electronic;
    }

    static void check(Object thing, List<Object> path, Object expected) {
        Object actual = getAtPath(thing, path);
        boolean ok = expected instanceof Type type
                ? type.type.isInstance(actual)
                : Objects.equals(expected, actual);

        if (!ok) {
            throw badRequest("Expected " + expected + " at " + path + ", got: " + (isTruthy(actual) ? actual : "<MISSING>"));
        }
    }

    enum Type {
        ARRAY(List.class),
        STRING(String.class);

        final Class<?> type;

        Type(Class<?> type) {
            this.type = type;
        }
    }

    static Map<String, Object> readJson(HttpServletRequest request) throws IOException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> json = mapper.readValue(request.getInputStream().readAllBytes(), Map.class);
            return json;
        } catch (JsonParseException e) {
            throw badRequest("Bad JSON: " + e.getMessage());
        }
    }

    static void sendError(HttpServletResponse response, int code, String msg) throws IOException {
        response.setStatus(code);
        response.setHeader("Content-Type", "application/json");
        mapper.writeValue(response.getOutputStream(), Map.of("code", code, "msg", msg != null ? msg : ""));
    }

    static String getXlAPI() {
        //FIXME
        int port = Configuration.getHttpPort();
        return "http://localhost:" + port + "/";
    }
}

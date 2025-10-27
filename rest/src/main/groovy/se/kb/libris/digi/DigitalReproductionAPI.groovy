package se.kb.libris.digi

import groovy.transform.MapConstructor
import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.map.ObjectMapper
import whelk.Configuration

import javax.servlet.ServletException
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.Semaphore

import static RequestException.badRequest
import static RequestException.internalError
import static Util.JSONLD
import static Util.asList
import static Util.asMap
import static Util.asSet
import static Util.getAtPath
import static Util.isLink
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST
import static javax.servlet.http.HttpServletResponse.SC_CREATED
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND
import static javax.servlet.http.HttpServletResponse.SC_NO_CONTENT
import static javax.servlet.http.HttpServletResponse.SC_OK
import static javax.servlet.http.HttpServletResponse.SC_PRECONDITION_FAILED
import static se.kb.libris.digi.DigitalReproductionAPI.Type.ARRAY
import static se.kb.libris.digi.DigitalReproductionAPI.Type.STRING

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

@Log
class DigitalReproductionAPI extends HttpServlet {
    static final String API_LOCATION = 'https://libris.kb.se/api/_reproduction' // Only for setting generationProcess 

    private static final ObjectMapper mapper = new ObjectMapper()

    private static final def FORWARD_HEADERS = [
            'XL-Active-Sigel',
            'Authorization'
    ].collect { it.toLowerCase() }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        def forwardHeaders = request.headerNames
                .findAll{ it.toLowerCase() in FORWARD_HEADERS }
                .collectEntries { [(it) : request.getHeader(it as String)] }

        def service = new ReproductionService(xl : new XL(headers : forwardHeaders, apiLocation: getXlAPI()))

        try {
            boolean extractWork = !Boolean.parseBoolean(request.getParameter("dont-extract-work"))
            String id = service.createDigitalReproduction(parse(request), extractWork)
            log.info("Created $id")
            response.setHeader('Location', id)
            response.setStatus(SC_CREATED)
        }
        catch (RequestException e) {
            log.warn("${e.code} ${e.msg}")
            sendError(response, e.code, e.msg)
        }
        catch (Exception e) {
            log.error("Internal error: $e", e)
            sendError(response, SC_INTERNAL_SERVER_ERROR, e.getMessage())
        }
    }

    static Map parse(HttpServletRequest request) {
        if (request.getHeader('Content-Type') != JSONLD) {
            throw badRequest("Header Content-Type must be $JSONLD")
        }

        def electronic = readJson(request)

        try {
            check(electronic, ['@type'], 'Electronic')
        } catch (RequestException ignored) {
            check(electronic, ['@type'], 'DigitalResource')
        }

        check(electronic, ['reproductionOf', '@id'], STRING)
        check(electronic, ['production'], ARRAY)
        if (!isLink(getAtPath(electronic, ['production', 0]))) { // minimal valid shape, so just check the first one 
            check(electronic, ['production', 0, '@type'], 'Reproduction')
            check(electronic, ['production', 0, 'date'], STRING)
            if (!isLink(getAtPath(electronic, ['production', 0, 'agent']))) {
                check(electronic, ['production', 0, 'place', '@type'], STRING)
            }
            if (!isLink(getAtPath(electronic, ['production', 0, 'place']))) {
                check(electronic, ['production', 0, 'place', '@type'], 'Place')
                check(electronic, ['production', 0, 'place', 'label'], STRING)
            }
        }

        return electronic
    }

    static void check(thing, path, expected) {
        def actual = getAtPath(thing, path)
        def ok = expected instanceof Type
                ? expected.type.isInstance(actual)
                : expected == actual

        if (!ok) {
            throw badRequest("Expected $expected at $path, got: ${actual ?: '<MISSING>'}")
        }
    }

    static enum Type {
        ARRAY(List.class),
        STRING(String.class)

        Class type
        Type(Class type) { this.type = type }
    }

    static Map readJson(HttpServletRequest request) {
        try {
            mapper.readValue(request.getInputStream().getBytes(), Map)
        } catch (JsonParseException e) {
            throw badRequest("Bad JSON: ${e.message}")
        }
    }

    static void sendError(HttpServletResponse response, int code, String msg) {
        response.setStatus(code)
        response.setHeader('Content-Type', 'application/json')
        mapper.writeValue(response.getOutputStream(), ['code': code, 'msg' : msg ?: ''])
    }

    static String getXlAPI() {
        //FIXME
        int port = Configuration.getHttpPort()
        "http://localhost:${port}/"
    }
}

@Log
class ReproductionService {
    private static final def DIGI = ['@id': 'https://libris.kb.se/library/DIGI']
    private static final def DST = ['@id': 'https://libris.kb.se/library/DST']
    private static final def ONLINE = ['@id': 'https://id.kb.se/term/rda/OnlineResource']
    private static final def FREELY_AVAILABLE = ['@id': 'https://id.kb.se/policy/freely-available']

    XL xl
    
    String createDigitalReproduction(Map electronicThing, boolean extractWork) {
        String requestedId = electronicThing.reproductionOf['@id'] as String
        Map physicalThing = xl.get(requestedId)
                .map{ it.data['@graph'][1] as Map }
                .orElseThrow{ badRequest("Thing linked in reproductionOf does not exist: $requestedId") }

        if (physicalThing['@type'] == 'Electronic') {
            throw badRequest("Thing linked in reproductionOf cannot be Electronic")
        }
        if (physicalThing['@type'] == 'DigitalResource') {
            throw badRequest("Thing linked in reproductionOf cannot be DigitalResource")
        }

        def holdingsToCreate = getAtPath(electronicThing, ['@reverse', 'itemOf'], [])
        def badHeldBy = holdingsToCreate
                .collect{ getAtPath(it, ['heldBy', '@id'], 'MISSING') }
                .findAll { xl.get(it).isEmpty() }
        if (badHeldBy) {
            throw badRequest("No such library: $badHeldBy")
        }
        
        electronicThing.reproductionOf['@id'] = physicalThing['@id'] // if link was to sameAs
        
        electronicThing.instanceOf = extractWork 
                ? ['@id' : xl.ensureExtractedWork(physicalThing['@id'] as String)] 
                : physicalThing.instanceOf
        
        if (physicalThing.hasTitle) {
            electronicThing.hasTitle = physicalThing.hasTitle
        }
        
        if (physicalThing.issuanceType && !electronicThing.issuanceType) {
            electronicThing.issuanceType = physicalThing.issuanceType
        }
        
        if (isOnline(electronicThing)) {
            if (electronicThing['@type'] == 'DigitalResource') {
                electronicThing.category = asSet(electronicThing.category) << ONLINE
            } else {
                electronicThing.carrierType = asSet(electronicThing.carrierType) << ONLINE
            }
        }

        def record = asMap(electronicThing.remove('meta'))

        record.bibliography = asSet(record.bibliography).with { bibliography ->
            if (isDigitaliseratSvensktTryck(physicalThing, electronicThing)) {
                bibliography << DST
            }
            bibliography << DIGI
        }
        
        def electronicId = xl.create(record, electronicThing)
        holdingsToCreate.each { item -> createHoldingFor(electronicId, item) }
        
        return electronicId
    }
    
    boolean isDigitaliseratSvensktTryck(Map physicalThing, Map electronicThing) {
        isFreelyAvailable(electronicThing) && physicalThing.publication?.any { Map p ->
            publishedIn(p, 'sw') || (publishedIn(p, 'fi') && pre1810(p))
        }
    }
    
    static boolean isFreelyAvailable(Map thing) {
        def usageAndAccess = asList(thing.usageAndAccessPolicy) + asList(getAtPath(thing, ['associatedMedia', '*', 'usageAndAccessPolicy'], []))
        usageAndAccess.any { it == FREELY_AVAILABLE }
    }

    static boolean publishedIn(Map publication, countryCode) {
        publication.country && publication.country['@id'] == "https://id.kb.se/country/$countryCode"
    }
    
    boolean pre1810(Map publication) {
        parseYear(publication.year as String) <= 1809 || parseYear(publication.date as String) <= 1809
    }

    static int parseYear(String date) {
        (date && date ==~ /\d\d\d\d.*/)
                ? Integer.parseInt(date.substring(0,4))
                : Integer.MAX_VALUE
    }

    static boolean isOnline(Map thing) {
        thing.hasRepresentation || !asList(getAtPath(thing, ['associatedMedia', '*', 'uri'], [])).isEmpty()
    }
    
    void createHoldingFor(thingId, Map item) {
        def record = asMap(item.remove('meta'))
        def heldById = getAtPath(item, ['heldBy', '@id'])
        
        def components = (asList(item.hasComponent) ?: [[:]]).collect {
            it + [
                    '@type' : 'Item',
                    'heldBy': ['@id' : heldById],
            ]
        }
        
        xl.create(
                record,
                item + [
                        '@type' : 'Item',
                        'itemOf' : ['@id' : thingId],
                        'heldBy': ['@id' : heldById],
                        'hasComponent' : components
                ]
        )
    }
}

@MapConstructor
@Log
class XL {
    // Since we are (for now) making HTTP requests to the same servlet container. must be lower that maxConnections / 2
    private static final int MAX_CONCURRENT_REQUESTS = 10 
    private static final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_REQUESTS)
    private static final int TIMEOUT_SECONDS = 60
    private static final HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build()
    private static final ObjectMapper mapper = new ObjectMapper()
    
    Map<String, String> headers
    String apiLocation

    String ensureExtractedWork(String instanceId) {
        int maxRetries = 5
        do {
            def doc = get(instanceId).orElseThrow { badRequest("No such record: $instanceId") }
            Map instance = doc.data['@graph'][1] as Map
            Map work = instance.instanceOf as Map

            if (!work) {
                throw badRequest("No instanceOf in $instanceId")
            }

            if (isLink(work)) {
                return work['@id']
            }

            if (!work.hasTitle) {
                def title = asList(instance.hasTitle).find { it['@type'] == 'Title' }
                if (title) {
                    work.hasTitle = [ title + [source: [['@id': instanceId]]] ] 
                }
            }

            def record = [
                    'generationProcess': ['@id': DigitalReproductionAPI.API_LOCATION],
                    'derivedFrom'      : [['@id': instanceId]]
            ]
            
            def workId = create(record, work)
            instance.instanceOf = ['@id': workId]
            try {
                update(doc)
                return workId
            } catch (RequestException e) {
                if (e.code == SC_PRECONDITION_FAILED) {
                    log.info("ensureExtractedWork() Document $instanceId was modified by someone else, " +
                             "deleting newly created work $workId and retrying")
                    delete(workId)
                }
                else {
                    throw e
                }
            }
        } while (maxRetries-- > 0)
        
        throw new RequestException(code: SC_PRECONDITION_FAILED)
    }

    void update(Doc doc) {
        String id = doc.data['@graph'][1]['@id']
        def request = requestForPath(id)
                .header('Content-Type', JSONLD)
                .header('If-Match', doc.eTag)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(doc.data)))
                .build()
        
        def response = send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != SC_NO_CONTENT) {
            throw new RequestException(code: response.statusCode(), msg: response.body())
        }
    }
    
    String create(Map record, Map thing) {
        def data = [
                '@graph': [
                        record + [
                                '@id'       : 'TEMP-ID',
                                '@type'     : 'Record',
                                'mainEntity': ['@id': 'TEMP-ID#it']
                        ],
                        thing + [
                                '@id': 'TEMP-ID#it',
                        ]
                ]
        ]

        def request = requestForPath('')
                .header('Content-Type', JSONLD)
                .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(data)))
                .build()
        
        def response = send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != SC_CREATED) {
            throw new RequestException(code: response.statusCode(), msg: response.body())
        }
        
        return response.headers().firstValue('Location').map{ it + '#it' }
                .orElseThrow{ internalError("Got no Location in create") }
    }
    
    Optional<Doc> get(String id) {
        def request = requestForPath("${id.split('#').first()}?embellished=false")
                .header('Accept', JSONLD)
                .GET()
                .build()
        
        def response = send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() == SC_NOT_FOUND) {
            return Optional.empty()
        }
        if (response.statusCode() != SC_OK) {
            throw new RequestException(code: response.statusCode(), msg: response.body())
        }
        
        return Optional.of(new Doc(
                data: mapper.readValue(response.body(), Map), 
                eTag: response.headers().firstValue('ETag').orElseThrow{ internalError("Got no ETag for $id") }
        ))
    }
    
    boolean delete(String id) {
        def request = requestForPath(id).DELETE().build()
        def response = send(request, HttpResponse.BodyHandlers.discarding())
        return response.statusCode() == SC_NO_CONTENT
    }
    
    HttpRequest.Builder requestForPath(String path) {
        def builder = HttpRequest.newBuilder()
                .uri(URI.create("$apiLocation$path"))
                .timeout(Duration.ofSeconds(TIMEOUT_SECONDS))
        
        headers.each { k, v ->
            builder.header(k, v)
        }
        
        return builder
    }

    static def send(request, responseBodyHandler) {
        try {
            semaphore.acquireUninterruptibly()
            log.debug(request)
            client.send(request, responseBodyHandler)
        }
       finally {
           semaphore.release()
       }
    }
    
    @MapConstructor
    static class Doc {
        Map data
        String eTag
    }
}

class Util {
    public static final String JSONLD = 'application/ld+json'
    
    static List asList(Object o) {
        (o instanceof List) ? (List) o : (o ? [o] : [])
    }

    static Map asMap(Object o) {
        (o instanceof Map) ? (Map) o : [:]
    }

    static Set asSet(Object o) {
        asList(o) as Set
    }

    static Object getAtPath(item, Iterable path, defaultTo = null) {
        if(!item) {
            return defaultTo
        }

        for (int i = 0 ; i < path.size(); i++) {
            def p = path[i]
            if (p == '*' && item instanceof Collection) {
                return item.collect { getAtPath(it, path.drop(i + 1), []) }.flatten()
            }
            else if (item[p] != null) {
                item = item[p]
            } else {
                return defaultTo
            }
        }
        return item
    }
    
    static boolean isLink(m) {
        m && m instanceof Map && m.'@id' && m.size() == 1
    }
}

@MapConstructor
class RequestException extends RuntimeException {
    int code
    String msg
    
    static RequestException badRequest(String msg) {
        new RequestException(code: SC_BAD_REQUEST, msg:  msg)
    }

    static RequestException internalError(String msg) {
        new RequestException(code: SC_INTERNAL_SERVER_ERROR, msg:  msg)
    }
}

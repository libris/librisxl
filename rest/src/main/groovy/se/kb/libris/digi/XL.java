package se.kb.libris.digi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_NO_CONTENT;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_PRECONDITION_FAILED;
import static se.kb.libris.digi.RequestException.badRequest;
import static se.kb.libris.digi.RequestException.internalError;
import static se.kb.libris.digi.Util.JSONLD;
import static se.kb.libris.digi.Util.asList;
import static se.kb.libris.digi.Util.isLink;
import static se.kb.libris.digi.Util.isTruthy;

class XL {
    private static final Logger log = LogManager.getLogger(XL.class);

    // Since we are (for now) making HTTP requests to the same servlet container. must be lower that maxConnections / 2
    private static final int MAX_CONCURRENT_REQUESTS = 10;
    private static final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_REQUESTS);
    private static final int TIMEOUT_SECONDS = 60;
    private static final HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build();
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Map<String, String> headers;
    private final String apiLocation;

    XL(Map<String, String> headers, String apiLocation) {
        this.headers = headers;
        this.apiLocation = apiLocation;
    }

    String ensureExtractedWork(String instanceId) throws IOException {
        int maxRetries = 5;
        do {
            Doc doc = get(instanceId).orElseThrow(() -> badRequest("No such record: " + instanceId));
            Map<String, Object> instance = graphEntity(doc.data());
            Map<String, Object> work = Util.asMap(instance.get("instanceOf"));

            if (!isTruthy(work)) {
                throw badRequest("No instanceOf in " + instanceId);
            }

            if (isLink(work)) {
                return (String) work.get("@id");
            }

            if (!isTruthy(work.get("hasTitle"))) {
                Map<String, Object> title = asList(instance.get("hasTitle")).stream()
                        .map(Util::asMap)
                        .filter(t -> "Title".equals(t.get("@type")))
                        .findFirst()
                        .orElse(null);
                if (title != null) {
                    Map<String, Object> withSource = new LinkedHashMap<>(title);
                    withSource.put("source", List.of(Map.of("@id", instanceId)));
                    work.put("hasTitle", new ArrayList<>(List.of(withSource)));
                }
            }

            Map<String, Object> record = new LinkedHashMap<>();
            record.put("generationProcess", Map.of("@id", DigitalReproductionAPI.API_LOCATION));
            record.put("derivedFrom", List.of(Map.of("@id", instanceId)));

            String workId = create(record, work);
            instance.put("instanceOf", new LinkedHashMap<>(Map.of("@id", workId)));
            try {
                update(doc);
                return workId;
            } catch (RequestException e) {
                if (e.code == SC_PRECONDITION_FAILED) {
                    log.info("ensureExtractedWork() Document {} was modified by someone else, "
                            + "deleting newly created work {} and retrying", instanceId, workId);
                    delete(workId);
                } else {
                    throw e;
                }
            }
        } while (maxRetries-- > 0);

        throw new RequestException(SC_PRECONDITION_FAILED);
    }

    void update(Doc doc) throws IOException {
        String id = (String) graphEntity(doc.data()).get("@id");
        HttpRequest request = requestForPath(id)
                .header("Content-Type", JSONLD)
                .header("If-Match", doc.eTag())
                .PUT(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(doc.data())))
                .build();

        HttpResponse<String> response = send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != SC_NO_CONTENT) {
            throw new RequestException(response.statusCode(), response.body());
        }
    }

    String create(Map<String, Object> record, Map<String, Object> thing) throws IOException {
        Map<String, Object> recordNode = new LinkedHashMap<>(record);
        recordNode.put("@id", "TEMP-ID");
        recordNode.put("@type", "Record");
        recordNode.put("mainEntity", Map.of("@id", "TEMP-ID#it"));

        Map<String, Object> thingNode = new LinkedHashMap<>(thing);
        thingNode.put("@id", "TEMP-ID#it");

        Map<String, Object> data = Map.of("@graph", List.of(recordNode, thingNode));

        HttpRequest request = requestForPath("")
                .header("Content-Type", JSONLD)
                .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(data)))
                .build();

        HttpResponse<String> response = send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != SC_CREATED) {
            throw new RequestException(response.statusCode(), response.body());
        }

        return response.headers().firstValue("Location").map(it -> it + "#it")
                .orElseThrow(() -> internalError("Got no Location in create"));
    }

    Optional<Doc> get(String id) throws IOException {
        HttpRequest request = requestForPath(id.split("#")[0] + "?embellished=false")
                .header("Accept", JSONLD)
                .GET()
                .build();

        HttpResponse<String> response = send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == SC_NOT_FOUND) {
            return Optional.empty();
        }
        if (response.statusCode() != SC_OK) {
            throw new RequestException(response.statusCode(), response.body());
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> data = mapper.readValue(response.body(), Map.class);
        String eTag = response.headers().firstValue("ETag")
                .orElseThrow(() -> internalError("Got no ETag for " + id));

        return Optional.of(new Doc(data, eTag));
    }

    boolean delete(String id) throws IOException {
        HttpRequest request = requestForPath(id).DELETE().build();
        HttpResponse<Void> response = send(request, HttpResponse.BodyHandlers.discarding());
        return response.statusCode() == SC_NO_CONTENT;
    }

    HttpRequest.Builder requestForPath(String path) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(apiLocation + path))
                .timeout(Duration.ofSeconds(TIMEOUT_SECONDS));

        headers.forEach(builder::header);

        return builder;
    }

    static <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
            throws IOException {
        try {
            semaphore.acquireUninterruptibly();
            log.debug(request);
            return client.send(request, responseBodyHandler);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw internalError("Interrupted: " + e.getMessage());
        } finally {
            semaphore.release();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> graphEntity(Map<String, Object> data) {
        return (Map<String, Object>) ((List<Object>) data.get("@graph")).get(1);
    }

    record Doc(Map<String, Object> data, String eTag) {}
}

package whelk.rest.api;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.IdGenerator;
import whelk.IdType;
import whelk.JsonLd;
import whelk.JsonLdValidator;
import whelk.TargetVocabMapper;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;
import whelk.exception.ElasticIOException;
import whelk.exception.InvalidQueryException;
import whelk.exception.StaleUpdateException;
import whelk.exception.UnexpectedHttpStatusException;
import whelk.exception.WhelkRuntimeException;
import whelk.history.History;
import whelk.rest.api.CrudGetRequest.Lens;
import whelk.rest.security.AccessControl;
import whelk.util.FresnelUtil;
import groovy.lang.Tuple2;
import whelk.util.http.BadRequestException;
import whelk.util.http.HttpTools;
import whelk.util.http.MimeTypes;
import whelk.util.http.NotFoundException;
import whelk.util.http.OtherStatusException;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static whelk.rest.api.CrudUtils.ETag;
import static whelk.util.Jackson.mapper;
import static whelk.util.http.HttpTools.getBaseUri;
import static whelk.util.http.HttpTools.sendResponse;

/**
 * Handles all GET/PUT/POST/DELETE requests against the backend.
 */
public class Crud extends WhelkHttpServlet {
    private static final Logger log = LogManager.getLogger(Crud.class);
    public static final String XL_ACTIVE_SIGEL_HEADER = "XL-Active-Sigel";
    public static final String CONTEXT_PATH = "/context.jsonld";
    public static final String DATA_CONTENT_TYPE = "application/ld+json";

    public static final RestMetrics metrics = new RestMetrics();

    private JsonLdValidator validator;
    private TargetVocabMapper targetVocabMapper;

    private AccessControl accessControl = new AccessControl();
    private ConverterUtils converterUtils;

    private SiteSearch siteSearch;

    private final Map<String, Tuple2<Document, String>> cachedFetches = new HashMap<>();

    public Crud() {
        // Do nothing - only here for Tomcat to have something to call
    }

    // For testing
    public Crud(Whelk whelk) {
        this.whelk = whelk;
        init(whelk);
    }

    @Override
    public void init(Whelk whelk) {
        siteSearch = new SiteSearch(whelk);
        validator = JsonLdValidator.from(whelk.getJsonld());
        converterUtils = new ConverterUtils(whelk);

        cacheFetchedResource(whelk.getSystemContextUri());
        cacheFetchedResource(whelk.getVocabUri());
        cacheFetchedResource(whelk.getVocabDisplayUri());

        Tuple2<Document, String> docAndLoc = cachedFetches.get(whelk.getSystemContextUri());
        if (docAndLoc != null) {
            Document contextDoc = docAndLoc.getV1();
            if (contextDoc != null) {
                targetVocabMapper = new TargetVocabMapper(whelk.getJsonld(), contextDoc.data);
            }
        }
    }

    protected void cacheFetchedResource(String resourceUri) {
        cachedFetches.put(resourceUri, getDocumentFromStorage(resourceUri, null));
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling GET request for {}", request.getPathInfo());
        try {
            doGet2(request, response);
        } catch (Exception e) {
            // Don't log a full callstack! This happens all the time when a user drops its connection
            // without having first received what it asked for (typically a slow query).
            log.info("Attempting to send error response, after catching: {}", e.toString());
            sendError(request, response, e);
        } finally {
            log.debug("Sending GET response with status {} for {}", 
                     response.getStatus(), request.getPathInfo());
        }
    }

    public void doGet2(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RestMetrics.Measurement measurement = null;
        try {
            if ("/".equals(request.getPathInfo())) {
                measurement = metrics.measure("INDEX");
                displayInfo(response);
            } else if (siteSearch.isSearchResource(request.getPathInfo())) {
                measurement = metrics.measure("FIND");
                handleQuery(request, response);
            } else {
                measurement = metrics.measure("GET");
                handleGetRequest(CrudGetRequest.parse(request), response);
            }
        } finally {
            if (measurement != null) {
                measurement.complete();
            }
        }
    }

    public void displayInfo(HttpServletResponse response) throws IOException {
        Map<String, Object> info = siteSearch.appsIndex.get(whelk.getApplicationId());
        sendResponse(response, mapper.writeValueAsString(info), "application/json");
    }

    public void handleQuery(HttpServletRequest request, HttpServletResponse response) {
        Map<String, String[]> queryParameters = new HashMap<>(request.getParameterMap());
        String baseUri = getBaseUri(request);

        try {
            Map<String, Object> results = siteSearch.findData(queryParameters, baseUri, request.getPathInfo());
            String uri = request.getRequestURI();
            Map<String, Object> contextData = whelk.getJsonld().context;
            CrudGetRequest crudReq = CrudGetRequest.parse(request);
            Object dataBody = getNegotiatedDataBody(crudReq, contextData, results, uri);

            sendGetResponse(response, dataBody, null, crudReq.getPath(), crudReq.getContentType(), crudReq.getId());
        } catch (ElasticIOException | UnexpectedHttpStatusException e) {
            log.error("Attempted elastic query, but failed: {}", e.toString(), e);
            throw new WhelkRuntimeException("Failed to reach elastic for query.", e);
        } catch (WhelkRuntimeException e) {
            log.error("Attempted elastic query, but whelk has no elastic component configured.", e);
            throw new OtherStatusException("Attempted to use elastic for query, but no elastic component is configured.",
                    HttpServletResponse.SC_NOT_IMPLEMENTED);
        } catch (InvalidQueryException e) {
            log.info("Invalid query: {}", queryParameters);
            throw new BadRequestException("Invalid query, please check the documentation. " + e.getMessage());
        }
    }

    public void handleGetRequest(CrudGetRequest request,
                          HttpServletResponse response) {
        Tuple2<Document, String> docAndLocation;

        if (cachedFetches.containsKey(request.getId())) {
            docAndLocation = cachedFetches.get(request.getId());
        } else {
            docAndLocation = getDocumentFromStorage(
                    request.getId(), request.getVersion().orElse(null));
        }

        Document doc = docAndLocation.getV1();
        String loc = docAndLocation.getV2();

        if (doc == null && loc == null) {
            sendNotFound(request.getHttpServletRequest(), response);
        } else if (doc == null && loc != null) {
            if (request.getView() == CrudGetRequest.View.DATA) {
                loc = getDataURI(loc, request);
            }
            sendRedirect(request.getHttpServletRequest(), response, loc);
        } else if (doc != null && doc.getDeleted()) {
            throw new OtherStatusException("Document has been deleted.", HttpServletResponse.SC_GONE);
        } else if (request.getView() == CrudGetRequest.View.CHANGE_SETS) {
            History history = new History(whelk.getStorage().loadDocumentHistory(doc.getShortId()), whelk.getJsonld());
            ETag eTag = ETag.plain(doc.getChecksum(whelk.getJsonld()));
            Map<String, Object> body = history.m_changeSetsMap;
            sendGetResponse(response, body, eTag, request.getPath(), request.getContentType(), request.getId());
        } else {
            ETag eTag;
            if (request.shouldEmbellish()) {
                String plainChecksum = doc.getChecksum(whelk.getJsonld());
                whelk.embellish(doc);
                eTag = ETag.embellished(plainChecksum, doc.getChecksum(whelk.getJsonld()));

                // reverse links are inserted by embellish, so can only do
                // this when embellished
                if (request.shouldApplyInverseOf()) {
                    doc.applyInverses(whelk.getJsonld());
                }
            } else {
                eTag = ETag.plain(doc.getChecksum(whelk.getJsonld()));
            }

            if (request.getIfNoneMatch().map(eTag::isNotModified).orElse(false)) {
                sendNotModified(response, eTag);
                return;
            }

            String profileId = request.getProfile().orElse(whelk.getSystemContextUri());
            addProfileHeaders(response, profileId);
            Object body = getFormattedResponseBody(request, doc, profileId);

            String location = loc != null ? loc : doc.getCompleteId();
            addProposal25Headers(response, location, getDataURI(location, request));

            sendGetResponse(response, body, eTag, request.getPath(), request.getContentType(), request.getId());
        }
    }

    private void sendNotModified(HttpServletResponse response, ETag eTag) {
        setVary(response);
        response.setHeader("ETag", eTag.toString());
        response.setHeader("Server-Start-Time", String.valueOf(ManagementFactory.getRuntimeMXBean().getStartTime()));
        HttpTools.sendError(response, HttpServletResponse.SC_NOT_MODIFIED, "Document has not been modified.");
    }

    private static void sendNotFound(HttpServletRequest request, HttpServletResponse response) {
        String method = request.getMethod();
        if (method != null) {
            metrics.failedRequests.labels(method, String.valueOf(HttpServletResponse.SC_NOT_FOUND)).inc();
        }
        HttpTools.sendError(response, HttpServletResponse.SC_NOT_FOUND, "Document not found.");
    }

    private Object getFormattedResponseBody(CrudGetRequest request, Document doc, String profileId) {
        log.debug("Formatting document {}. embellished: {}, framed: {}, lens: {}, view: {}, profile: {}",
                doc.getCompleteId(),
                request.shouldEmbellish(),
                request.shouldFrame(),
                request.getLens(),
                request.getView(),
                profileId);

        Map<String, Object> data;
        if (request.getLens() != Lens.NONE) {
            data = applyLens(frameThing(doc), request.getLens());
        } else {
            data = request.shouldFrame() ? frameRecord(doc) : doc.data;
        }

        Object contextData = whelk.getJsonld().context;
        if (!profileId.equals(whelk.getSystemContextUri())) {
            data = applyDataProfile(profileId, data);
            contextData = data.get(JsonLd.CONTEXT_KEY);
            data.put(JsonLd.CONTEXT_KEY, profileId);
        }

        String uri = MimeTypes.TRIG.equals(request.getContentType()) ? doc.getCompleteId() : doc.getShortId();
        return getNegotiatedDataBody(request, contextData, data, uri);
    }

    private Object getNegotiatedDataBody(CrudGetRequest request, Object contextData, Map<String, Object> data, String uri) {
        if (request.shouldComputeLabels()) {
            if (!JsonLd.isFramed(data)) {
                // TODO? should we support this? Requires more work in FresnelUtil
                throw new BadRequestException("Cannot compute labels when not framed");
            }

            // FIXME FresnelUtil can't handle the whole search response because of @container @index in stats
            // TODO at least compute labels in stats observations and search mappings predicate/object?
            if (siteSearch.isSearchResource(request.getHttpServletRequest().getPathInfo())) {
                @SuppressWarnings("unchecked")
                List<Object> items = (List<Object>) data.get("items");
                whelk.getFresnelUtil().insertComputedLabels(items, new FresnelUtil.LangCode(request.computedLabelLocale()));
            } else {
                whelk.getFresnelUtil().insertComputedLabels(data, new FresnelUtil.LangCode(request.computedLabelLocale()));
            }
        }

        List<String> acceptedTypes = Arrays.asList(MimeTypes.JSON, MimeTypes.JSONLD);
        if (!acceptedTypes.contains(request.getContentType())) {
            data.put(JsonLd.CONTEXT_KEY, contextData);
            return converterUtils.convert(data, uri, request.getContentType());
        } else {
            return data;
        }
    }

    private static Map<String, Object> frameRecord(Document document) {
        return JsonLd.frame(document.getCompleteId(), document.data, 3);
    }

    private static Map<String, Object> frameThing(Document document) {
        document.setThingMeta(document.getCompleteId());
        List<String> thingIds = document.getThingIdentifiers();
        if (thingIds.isEmpty()) {
            String msg = "Could not frame document. Missing mainEntity? In: " + document.getCompleteId();
            log.warn(msg);
            throw new WhelkRuntimeException(msg);
        }
        return JsonLd.frame(thingIds.get(0), document.data);
    }
    
    private Map<String, Object> applyLens(Map<String, Object> framedThing, Lens lens) {
        return switch (lens) {
            case NONE -> framedThing;
            case CARD -> whelk.getJsonld().toCard(framedThing);
            case CHIP -> (Map<String, Object>) whelk.getJsonld().toChip(framedThing);
        };
    }

    private static void setVary(HttpServletResponse response) {
        response.setHeader("Vary", "Accept, Accept-Profile");
    }

    /**
     * Return request URI
     *
     */
    static String getRequestPath(HttpServletRequest request) {
        String path = request.getRequestURI();
        // Tomcat incorrectly strips away double slashes from the pathinfo.
        // Compensate here.
        if (path.matches("/http:/[^/].+")) {
            path = path.replace("http:/", "http://");
        } else if (path.matches("/https:/[^/].+")) {
            path = path.replace("https:/", "https://");
        }

        return path;
    }

    /**
     * Get (document, location) from storage for specified ID and version.
     *
     * If version is null, we look for the latest version.
     * Document and String in the response may be null.
     *
     */
    public Tuple2<Document, String> getDocumentFromStorage(String id, String version) {
        Tuple2<Document, String> result = new Tuple2<>(null, null);

        Document doc = whelk.getStorage().load(id, version);
        if (doc != null) {
            // it has been merged with another doc - main id is in sameAs of remaining doc
            // TODO this could/should be implemented as a replacedBy inside the tombstone?
            if (doc.getDeleted() && !JsonLd.looksLikeIri(id)) {
                String iri = Document.getBASE_URI().toString() + id + Document.HASH_IT;
                String location = whelk.getStorage().getMainId(iri);
                if (location != null) {
                    return new Tuple2<>(null, location);
                }
            }

            return new Tuple2<>(doc, null);
        }

        // we couldn't find the document directly, so we look it up using the
        // identifiers table instead
        IdType idType = whelk.getStorage().getIdType(id);
        if (idType != null) {
            switch (idType) {
                case RecordMainId -> {
                    doc = whelk.getStorage().loadDocumentByMainId(id, version);
                    if (doc != null) {
                        result = new Tuple2<>(doc, null);
                    }
                }
                case ThingMainId -> {
                    doc = whelk.getStorage().loadDocumentByMainId(id, version);
                    if (doc != null) {
                        String contentLocation = whelk.getStorage().getRecordId(id);
                        result = new Tuple2<>(doc, contentLocation);
                    }
                }
                case RecordSameAsId, ThingSameAsId -> {
                    String location = whelk.getStorage().getMainId(id);
                    if (location != null) {
                        result = new Tuple2<>(null, location);
                    }
                }
                default -> {
                    // 404
                }
            }
        }
        // If idType is null, we fall through to return the result (which will be null, null) - 404

        return result;
    }

    // Overloaded method for backward compatibility
    public Tuple2<Document, String> getDocumentFromStorage(String id) {
        return getDocumentFromStorage(id, null);
    }

    /**
     * See <https://www.w3.org/wiki/HTML/ChangeProposal25>
     */
    private static void addProposal25Headers(HttpServletResponse response,
                                             String location,
                                             String dataLocation) {
        response.addHeader("Content-Location", dataLocation);
        response.addHeader("Document", location);
        response.addHeader("Link", "<" + location + ">; rel=describedby");
    }

    /**
     * TODO: Spec in flux; see status at:
     * <https://www.w3.org/TR/dx-prof-conneg/>
     * and:
     * <https://profilenegotiation.github.io/I-D-Profile-Negotiation/I-D-Profile-Negotiation.html>
     */
    private static void addProfileHeaders(HttpServletResponse response, String profileId) {
        response.setHeader("Content-Profile", "<" + profileId + ">");
        response.setHeader("Link", "<" + profileId + ">; rel=\"profile\"");
    }

    private static String getDataURI(String location, CrudGetRequest request) {
        String lens = request.getLens() == Lens.NONE ? null : request.getLens().toString().toLowerCase();
        return getDataURI(location, request.getContentType(), lens, request.getProfile().orElse(null));
    }

    private static String getDataURI(String location,
                                     String contentType,
                                     String lens,
                                     String profile) {
        if (contentType == null) {
            return location;
        }
        
        if (location.endsWith("#it")) {
            // We should normally never get '#it' URIs here since /data should only work on records 
            location = location.substring(0, location.length() - "#it".length());
        }
        
        StringBuilder loc = new StringBuilder(location);

        String slash = location.endsWith("/") ? "" : "/";
        String ext = CrudUtils.EXTENSION_BY_MEDIA_TYPE.getOrDefault(contentType, "jsonld");

        loc.append(slash).append("data.").append(ext);

        StringJoiner params = new StringJoiner("&");
        if (lens != null) {
            params.add("lens=" + lens);
        }
        if (profile != null) {
            params.add("profile=" + profile);
        }
        if (params.length() > 0) {
            loc.append("?").append(params.toString());
        }

        return loc.toString();
    }

    public Map<String, Object> applyDataProfile(String profileId, Map<String, Object> data) {
        Tuple2<Document, String> docAndLoc = getDocumentFromStorage(profileId);
        Document profileDoc = docAndLoc.getV1();
        if (profileDoc == null) {
            throw new BadRequestException("Profile <" + profileId + "> is not available");
        }
        log.debug("Using profile: {}", profileId);
        Map<String, Object> contextDoc = profileDoc.data;
        data = (Map<String, Object>) targetVocabMapper.applyTargetVocabularyMap(profileId, contextDoc, data);
        data.put(JsonLd.CONTEXT_KEY, contextDoc.get(JsonLd.CONTEXT_KEY));

        return data;
    }

    /**
     * Format and send GET response to client.
     *
     * Sets the necessary headers and picks the best Content-Type to use.
     *
     */
    public void sendGetResponse(HttpServletResponse response,
                         Object responseBody, ETag eTag, String path,
                         String contentType, String requestId) {
        if (MimeTypes.JSON.equals(contentType)) {
            response.setHeader("Link",
                    "<" + CONTEXT_PATH + ">; " +
                            "rel=\"http://www.w3.org/ns/json-ld#context\"; " +
                            "type=\"application/ld+json\"");
        } else if (MimeTypes.JSONLD.equals(contentType) && responseBody instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mapBody = (Map<String, Object>) responseBody;
            if (!mapBody.containsKey(JsonLd.CONTEXT_KEY)) {
                mapBody.put(JsonLd.CONTEXT_KEY, CONTEXT_PATH);
            }
        }

        if (eTag != null) {
            response.setHeader("ETag", eTag.toString());
        }

        setVary(response);

        if (responseBody instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mapResponse = (Map<String, Object>) responseBody;
            sendResponse(response, mapResponse, contentType);
        } else {
            sendResponse(response, (String) responseBody, contentType);
        }
    }

    /**
     * Send 302 Found response
     *
     */
    public static void sendRedirect(HttpServletRequest request,
                             HttpServletResponse response, String location) {
        try {
            if (new URI(location).getScheme() == null) {
                String locationRef = request.getScheme() + "://" +
                        request.getServerName() +
                        (request.getServerPort() != 80 ? ":" + request.getServerPort() : "") +
                        request.getContextPath();
                location = locationRef + location;
            }
            response.setHeader("Location", location);
            log.debug("Redirecting to document location: {}", location);
            sendResponse(response, new byte[0], null, HttpServletResponse.SC_FOUND);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create redirect URI", e);
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) {
        RestMetrics.Measurement measurement = metrics.measure("POST");
        log.debug("Handling POST request for {}", request.getPathInfo());

        try {
            doPost2(request, response);
        } catch (Exception e) {
            sendError(request, response, e);
        } finally {
            measurement.complete();
            log.info("Sending POST response with status {} for {}", 
                     response.getStatus(), request.getPathInfo());
        }
    }

    public void doPost2(HttpServletRequest request, HttpServletResponse response) {
        if (!"/".equals(request.getPathInfo()) && !"/data".equals(request.getPathInfo())) {
            throw new OtherStatusException("Method not allowed.", HttpServletResponse.SC_METHOD_NOT_ALLOWED);
        }
        if (!isSupportedContentType(request.getContentType())) {
            throw new BadRequestException("Content-Type not supported.");
        }

        Map<String, Object> requestBody = getRequestBody(request);

        if (isEmptyInput(requestBody)) {
            throw new BadRequestException("No data received.");
        }
        if (!JsonLd.isFlat(requestBody)) {
            throw new BadRequestException("Body is not flat JSON-LD.");
        }

        // FIXME we're assuming Content-Type application/ld+json here
        // should we deny the others?

        Document newDoc = new Document(requestBody);

        if (newDoc.getId() == null) {
            throw new BadRequestException("Document is missing temporary @id in Record");
        }
        if (newDoc.getThingIdentifiers().isEmpty()) {
            throw new BadRequestException("Document is missing temporary mainEntity.@id in Record");
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> graph = (List<Map<String, Object>>) newDoc.data.get("@graph");
        if (graph.size() < 2 || graph.get(1) == null || graph.get(1).get("@id") == null) {
            throw new BadRequestException("Document is missing temporary @id in Thing");
        }

        String firstThingId = newDoc.getThingIdentifiers().get(0);
        if (!firstThingId.equals(graph.get(1).get("@id"))) {
            throw new BadRequestException("The Record's temporary mainEntity.@id is not same as the Thing's temporary @id");
        }

        if (newDoc.getId().equals(firstThingId)) {
            throw new BadRequestException("The Record's temporary @id can't be the same as the Thing's temporary mainEntity.@id");
        }

        newDoc.normalizeUnicode();
        newDoc.deepReplaceId(Document.getBASE_URI().toString() + IdGenerator.generate());
        newDoc.setControlNumber(newDoc.getShortId());
        
        List<JsonLdValidator.Error> errors = validator.validate(newDoc.data, newDoc.getLegacyCollection(whelk.getJsonld()));
        if (!errors.isEmpty()) {
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("errors", errors.stream().map(JsonLdValidator.Error::toMap).toList());
            throw new BadRequestException("Invalid JSON-LD", errorMap);
        }
        
        // verify user permissions
        @SuppressWarnings("unchecked")
        Map<String, Object> userInfo = (Map<String, Object>) request.getAttribute("user");
        if (!hasPostPermission(newDoc, userInfo)) {
            throw new OtherStatusException("You are not authorized to perform this operation", HttpServletResponse.SC_FORBIDDEN);
        }
        
        // try store document
        // return 201 or error
        boolean isUpdate = false;
        Document savedDoc = saveDocument(newDoc, request, isUpdate);
        if (savedDoc != null) {
            sendCreateResponse(response, savedDoc.getURI().toString(),
                    ETag.plain(savedDoc.getChecksum(whelk.getJsonld())));
        } else if (!response.isCommitted()) {
            sendNotFound(request, response);
        }
    }

    @Override
    public void doPut(HttpServletRequest request, HttpServletResponse response) {
        RestMetrics.Measurement measurement = metrics.measure("PUT");
        log.debug("Handling PUT request for {}", request.getPathInfo());

        try {
            doPut2(request, response);
        } catch (Exception e) {
            sendError(request, response, e);
        } finally {
            measurement.complete();
            log.info("Sending PUT response with status {} for {}", 
                     response.getStatus(), request.getPathInfo());
        }
    }

    public void doPut2(HttpServletRequest request, HttpServletResponse response) {
        if ("/".equals(request.getPathInfo())) {
            throw new OtherStatusException("Method not allowed.", HttpServletResponse.SC_METHOD_NOT_ALLOWED);
        }
        if (!isSupportedContentType(request.getContentType())) {
            throw new BadRequestException("Content-Type not supported.");
        }

        Map<String, Object> requestBody = getRequestBody(request);

        if (isEmptyInput(requestBody)) {
            throw new BadRequestException("No data received.");
        }

        String documentId = JsonLd.findIdentifier(requestBody);
        String idFromUrl = getRequestPath(request).substring(1);

        if (documentId == null) {
            throw new BadRequestException("Missing @id in request.");
        }

        Tuple2<Document, String> docAndLoc = getDocumentFromStorage(idFromUrl);
        Document existingDoc = docAndLoc.getV1();
        String location = docAndLoc.getV2();

        if (existingDoc == null && location == null) {
            throw new NotFoundException("Document not found.");
        } else if (existingDoc == null && location != null) {
            sendRedirect(request, response, location);
            return;
        } else {
            String fullPutId = JsonLd.findFullIdentifier(requestBody);
            if (!fullPutId.equals(existingDoc.getCompleteId())) {
                log.debug("Record ID for {} changed to {} in PUT body", 
                          existingDoc.getCompleteId(), fullPutId);
                throw new BadRequestException("Record ID was modified");
            }
        }

        Document updatedDoc = new Document(requestBody);
        updatedDoc.normalizeUnicode();
        updatedDoc.setId(documentId);
        
        List<JsonLdValidator.Error> errors = validator.validate(updatedDoc.data, updatedDoc.getLegacyCollection(whelk.getJsonld()));
        if (!errors.isEmpty()) {
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("errors", errors.stream().map(JsonLdValidator.Error::toMap).toList());
            throw new BadRequestException("Invalid JSON-LD", errorMap);
        }

        log.debug("Checking permissions for {}", updatedDoc);
        // TODO: 'collection' must also match the collection 'existingDoc' is in.
        @SuppressWarnings("unchecked")
        Map<String, Object> userInfo = (Map<String, Object>) request.getAttribute("user");
        if (!hasPutPermission(updatedDoc, existingDoc, userInfo)) {
            throw new OtherStatusException("You are not authorized to perform this operation", HttpServletResponse.SC_FORBIDDEN);
        }
        
        boolean isUpdate = true;
        Document savedDoc = saveDocument(updatedDoc, request, isUpdate);
        if (savedDoc != null) {
            sendUpdateResponse(response, savedDoc.getURI().toString(),
                    ETag.plain(savedDoc.getChecksum(whelk.getJsonld())));
        }
    }

    public static boolean isEmptyInput(Map<String, Object> inputData) {
        return inputData == null || inputData.isEmpty();
    }

    public static boolean isSupportedContentType(String contentType) {
        String mimeType = ContentType.parse(contentType).getMimeType();
        return DATA_CONTENT_TYPE.equals(mimeType);
    }

    public static Map<String, Object> getRequestBody(HttpServletRequest request) {
        try {
            byte[] body = request.getInputStream().readAllBytes();
            return mapper.readValue(body, Map.class);
        } catch (EOFException e) {
            return new HashMap<>();
        } catch (IOException e) {
            throw new BadRequestException("Failed to read request body: " + e.getMessage());
        }
    }

    public Document saveDocument(Document doc, HttpServletRequest request, boolean isUpdate) {
        try {
            if (doc != null) {
                String activeSigel = request.getHeader(XL_ACTIVE_SIGEL_HEADER);
                String collection = doc.getLegacyCollection(whelk.getJsonld());
                if (isUpdate) {
                    // You are not allowed to change collection when updating a record
                    if (!collection.equals(whelk.getStorage().getCollectionBySystemID(doc.getShortId()))) {
                        log.info("Refused API update of document due to changed 'collection'");
                        throw new BadRequestException("Cannot change legacy collection for document. Legacy collection is mapped from entity @type.");
                    }

                    ETag ifMatch = Optional
                            .ofNullable(request.getHeader("If-Match"))
                            .map(ETag::parse)
                            .orElseThrow(() -> new BadRequestException("Missing If-Match header in update"));
                    
                    log.info("If-Match: {}", ifMatch);
                    whelk.storeAtomicUpdate(doc, false, true, "xl", activeSigel, ifMatch.documentCheckSum());
                }
                else {
                    log.debug("Saving NEW document ({})", doc.getId());
                    boolean success = whelk.createDocument(doc, "xl", activeSigel, collection, false);
                    if (!success) {
                        return null;
                    }
                }

                log.debug("Saving document ({})", doc.getShortId());
                log.info("Document accepted: created is: {}", doc.getCreated());

                return doc;
            }
        } catch (Exception e) {
            // TODO: catch (and throw) the actual exceptions once more code has been de-Groovied.
            if (e.getClass().getSimpleName().equals("StaleUpdateException")) {
                throw new OtherStatusException("The resource has been updated by someone else. Please refetch.", HttpServletResponse.SC_PRECONDITION_FAILED);
            } else if (e.getClass().getName().contains("AcquireLockException")) {
                throw new BadRequestException("Failed to acquire a necessary lock. Did you submit a holding record without a valid bib link? " + e.getMessage());
            } else {
                throw e;
            }
        }
        return null;
    }

    public static void sendCreateResponse(HttpServletResponse response, String locationRef, ETag eTag) {
        sendDocumentSavedResponse(response, locationRef, eTag, true);
    }

    public static void sendUpdateResponse(HttpServletResponse response, String locationRef, ETag eTag) {
        sendDocumentSavedResponse(response, locationRef, eTag, false);
    }

    public static void sendDocumentSavedResponse(HttpServletResponse response,
                                          String locationRef, 
                                          ETag eTag,
                                          boolean newDocument) {
        log.debug("Setting header Location: {}", locationRef);

        response.setHeader("Location", locationRef);
        response.setHeader("ETag", eTag.toString());
        response.setHeader("Cache-Control", "no-cache");

        if (newDocument) {
            response.setStatus(HttpServletResponse.SC_CREATED);
        } else {
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    public boolean hasPostPermission(Document newDoc, Map<String, Object> userInfo) {
        if (userInfo != null) {
            log.debug("User is: {}", userInfo);
            if (isSystemUser(userInfo)) {
                return true;
            } else {
                return accessControl.checkDocumentToPost(newDoc, userInfo, whelk.getJsonld());
            }
        }
        log.info("No user information received, denying request.");
        return false;
    }

    public boolean hasPutPermission(Document newDoc, Document oldDoc, Map<String, Object> userInfo) {
        if (userInfo != null) {
            log.debug("User is: {}", userInfo);
            if (isSystemUser(userInfo)) {
                return true;
            } else {
                return accessControl.checkDocumentToPut(newDoc, oldDoc, userInfo, whelk.getJsonld());
            }
        }
        log.info("No user information received, denying request.");
        return false;
    }

    public boolean hasDeletePermission(Document oldDoc, Map<String, Object> userInfo) {
        if (userInfo != null) {
            log.debug("User is: {}", userInfo);
            if (isSystemUser(userInfo)) {
                return true;
            } else {
                return accessControl.checkDocumentToDelete(oldDoc, userInfo, whelk.getJsonld());
            }
        }
        log.info("No user information received, denying request.");
        return false;
    }

    public static boolean isSystemUser(Map<String, Object> userInfo) {
        if ("SYSTEM".equals(userInfo.get("user"))) {
            log.warn("User is SYSTEM. Allowing access to all.");
            return true;
        }

        return false;
    }

    @Override
    public void doDelete(HttpServletRequest request, HttpServletResponse response) {
        RestMetrics.Measurement measurement = metrics.measure("DELETE");
        log.debug("Handling DELETE request for {}", request.getPathInfo());

        try {
            doDelete2(request, response);
        } catch (Exception e) {
            sendError(request, response, e);
        } finally {
            measurement.complete();
            log.debug("Sending DELETE response with status {} for {}", 
                     response.getStatus(), request.getPathInfo());
        }
    }

    public void doDelete2(HttpServletRequest request, HttpServletResponse response) {
        if ("/".equals(request.getPathInfo())) {
            throw new OtherStatusException("Method not allowed.", HttpServletResponse.SC_METHOD_NOT_ALLOWED);
        }
        String id = getRequestPath(request).substring(1);
        Tuple2<Document, String> docAndLocation = getDocumentFromStorage(id);
        Document doc = docAndLocation.getV1();
        String loc = docAndLocation.getV2();

        if (doc == null && loc != null) {
            sendRedirect(request, response, loc);
        } else if (doc == null) {
            throw new NotFoundException("Document not found.");
        } else {
            @SuppressWarnings("unchecked")
            Map<String, Object> userInfo = (Map<String, Object>) request.getAttribute("user");
            if (!hasDeletePermission(doc, userInfo)) {
                throw new OtherStatusException("You do not have sufficient privileges to perform this operation.", HttpServletResponse.SC_FORBIDDEN);
            } else if (doc.getDeleted()) {
                throw new OtherStatusException("Document has been deleted.", HttpServletResponse.SC_GONE);
            } else {
                log.debug("Removing resource at {}", doc.getShortId());
                String activeSigel = request.getHeader(XL_ACTIVE_SIGEL_HEADER);
                whelk.remove(doc.getShortId(), "xl", activeSigel);
                response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            }
        }
    }

    public static void sendError(HttpServletRequest request, HttpServletResponse response, Exception e) {
        int code = HttpTools.mapError(e);
        String method = request.getMethod();
        if (method != null) {
            metrics.failedRequests.labels(method, String.valueOf(code)).inc();
        }
        if (log.isDebugEnabled()) {
            String requestURI = request.getRequestURI();
            log.debug("Sending error {} : {} for {}", code, e.getMessage(), requestURI != null ? requestURI : "unknown");
        }
        HttpTools.sendError(response, code, e.getMessage(), e);
    }
}

package whelk.rest.api

import groovy.util.logging.Slf4j as Log
import org.apache.http.entity.ContentType
import org.codehaus.jackson.map.ObjectMapper
import org.picocontainer.PicoContainer
import whelk.Document
import whelk.JsonLd
import whelk.Location
import whelk.Whelk
import whelk.IdGenerator
import whelk.component.ElasticSearch
import whelk.component.StorageType
import whelk.converter.FormatConverter
import whelk.converter.marc.JsonLD2MarcConverter
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.exception.ModelValidationException
import whelk.exception.StorageCreateFailedException
import whelk.exception.WhelkAddException
import whelk.exception.WhelkRuntimeException
import whelk.exception.WhelkStorageException
import whelk.rest.api.CrudUtils
import whelk.rest.api.MimeTypes
import whelk.rest.api.SearchUtils
import whelk.rest.security.AccessControl
import whelk.util.PropertyLoader

import javax.activation.MimetypesFileTypeMap
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static HttpTools.sendResponse
import static whelk.rest.api.HttpTools.getMajorContentType

/**
 * Handles all GET/PUT/POST/DELETE requests against the backend.
 *
 * Created by markus on 2015-10-09.
 */
@Log
class Crud extends HttpServlet {

    final static String SAMEAS_NAMESPACE = "http://www.w3.org/2002/07/owl#sameAs"
    final static String DOCBASE_URI = "http://libris.kb.se/"

    enum FormattingType {
        EMBELLISHED, RAW
    }

    static final JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter();

    MimetypesFileTypeMap mimeTypes = new MimetypesFileTypeMap()

    final static Map contextHeaders = [
            "bib": "/sys/context/lib.jsonld",
            "auth": "/sys/context/lib.jsonld",
            "hold": "/sys/context/lib.jsonld"
    ]
    Whelk whelk
    PicoContainer pico
    static final ObjectMapper mapper = new ObjectMapper()
    AccessControl accessControl = new AccessControl()

    Crud() {
        super()
        log.info("Setting up httpwhelk.")

        Properties props = PropertyLoader.loadProperties("secret")

        // Get a properties pico container, pre-wired with components according to components.properties
        pico = Whelk.getPreparedComponentsContainer(props)

        pico.addComponent(JsonLD2MarcConverter.class)
        pico.addComponent(JsonLD2MarcXMLConverter.class)
        pico.addComponent(ISXNTool.class)

        //pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(ApixClientCamel.class)
        //pico.addComponent(Characteristics.CACHE).addComponent(JsonLdLinkExpander.class)

        pico.start()
    }

    @Override
    void init() {
        whelk = pico.getComponent(Whelk.class)
    }

    StorageType autoDetectQueryMode(Map queries) {
        boolean probablyMarcQuery = false
        for (entry in queries) {
            if (entry.key ==~ /\d{3}\.{0,1}\w{0,1}/) {
                probablyMarcQuery = true
            } else if (!entry.key.startsWith("_")) {
                probablyMarcQuery = false
            }
        }
        return probablyMarcQuery ? StorageType.MARC21_JSON : StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS
    }

    void handleQuery(HttpServletRequest request, HttpServletResponse response,
                     String dataset, String siteBaseUri=null) {
        Map queryParameters = new HashMap<String, String[]>(request.getParameterMap())
        String callback = queryParameters.remove("callback")

        try {
            Map results = SearchUtils.doSearch(whelk, queryParameters, dataset,
                                               siteBaseUri)
            def jsonResult

            if (callback) {
                jsonResult = callback + "(" +
                             mapper.writeValueAsString(results) + ");"
            } else {
                jsonResult = mapper.writeValueAsString(results)
            }

            sendResponse(response, jsonResult, "application/json")
        } catch (WhelkRuntimeException wse) {
            log.error("Attempted elastic query, but whelk has no " +
                      "elastic component configured.")
            response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED,
                               "Attempted to use elastic for query, but " +
                               "no elastic component is configured.")
            return
        }
    }

    void displayInfo(response) {
        def info = [:]
        info["system"] = "LIBRISXL"
        info["version"] = whelk.storage.loadSettings("system").get("version")
        info["format"] = "linked-data-api"
        //info["collections"] = whelk.storage.loadCollections()
        sendResponse(response, mapper.writeValueAsString(info), "application/json")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling GET request.")
        if (request.pathInfo == "/") {
            displayInfo(response)
            return
        }

        if (request.pathInfo == "/find") {
            String collection = request.getParameter("collection")
            handleQuery(request, response, collection)
            return
        }

        try {
            def path = request.pathInfo

            // FIXME still true/needed?
            // Tomcat incorrectly strips away double slashes from the pathinfo.
            // Compensate here.
            if (path ==~ "/http:/[^/].+")
            {
                path = path.replace("http:/", "http://")
            }

            handleGetRequest(request, response, path, getFormattingType(path))
        } catch (UnsupportedContentTypeException ucte) {
            response.sendError(response.SC_NOT_ACCEPTABLE, ucte.message)
        } catch (WhelkRuntimeException wrte) {
            response.sendError(response.SC_INTERNAL_SERVER_ERROR, wrte.message)
        }
    }

    void handleGetRequest(HttpServletRequest request,
                          HttpServletResponse response,
                          String path, FormattingType format) {
        String id = getIdFromPath(path)
        String version = request.getParameter("version")

        Tuple2 docAndLocation = getDocumentFromStorage(id, version)
        Document doc = docAndLocation.first
        Location loc = docAndLocation.second

        if (!doc && !loc) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                               "Document not found.")
            return
        } else if (!doc && loc) {
            log.debug("Redirecting to document location: ${loc.uri}")
            sendRedirect(request, response, loc)
            return
        }

        if (format == FormattingType.EMBELLISHED) {
            List externalRefs = doc.getExternalRefs()
            Map referencedDocuments = getDocuments(externalRefs)
            doc.embellish(referencedDocuments)
        }

        sendGetResponse(request, response, doc, path)
    }

    /**
     * Given a resource path, return the ID part or null.
     *
     * E.g.:
     * getIdFromPath("/foo") -> "foo"
     * getIdFromPath("/foo/description.jsonld") -> "foo"
     * getIdFromPath("/") -> null
     *
     */
    static String getIdFromPath(String path) {
        def pattern = getPathRegex()
        def matcher = path =~ pattern
        if (matcher.matches()) {
            return matcher[0][1]
        } else {
            return null
        }
    }

    /**
     * Given a resource path, return the formatting type for that resource.
     *
     * FormattingType.EMBELLISHED is the default return value.
     *
     * E.g.:
     * getFormattingType("/foo") -> FormattingType.EMBELLISHED
     * getFormattingType("/foo/description") -> FormattingType.RAW
     * getFormattingType("/") -> FormattingType.EMBELLISHED
     *
     */
    static FormattingType getFormattingType(String path) {
        def pattern = getPathRegex()
        def matcher = path =~ pattern
        if (matcher.matches() && matcher[0][2]) {
            return FormattingType.RAW
        } else {
            return FormattingType.EMBELLISHED
        }
    }

    /**
     * Return a regex pattern representation of a CRUD path.
     *
     * Matches /foo, /foo/description, /foo/description.<suffix>.
     *
     */
    static String getPathRegex() {
        return ~/^\/(.+?)(\/description(\.(\w+))?)?$/
    }

    /**
     * Get (Document, Location) from storage for specified ID and version.
     *
     * If version is null, we look for the latest version.
     * Document and Location in the response may be null.
     *
     */
    Tuple2 getDocumentFromStorage(String id, String version) {
        if (version) {
            Document doc = whelk.storage.load(id, version)
            return new Tuple2(doc, null)
        } else {
            // FIXME what does this mean?
            // We don't want to load the document here
            Location location = whelk.storage.locate(id, false)
            Document document = location?.document
            if (!document && location?.uri) {
                return new Tuple2(null, location)
            } else if(!document) {
                return new Tuple2(null, null)
            } else {
                return new Tuple2(document, location)
            }
        }
    }

    /**
     *
     * for each entry in search results
     *      for each part of entry
     *          get fields to include for @type
     *          strip out everything else
     *          try to expand things ((using cache first), then ES)
     */
    Map toCard(Map framedResults, Map displayData){
        def cleanedList = []
        Map result = framedResults

        //Used for the defintions of the cards and chips
        Map lensGroups = displayData.get("lensGroups")
        Map cards = lensGroups.get("cards")
        Map chips = lensGroups.get("chips")

        //only for framed data
        if (framedResults.containsKey("items")) {

            framedResults.get("items").each {item ->
                Map json = removeProperties(item, cards)
                Map json2 = toChip(json, chips)
                cleanedList << json2
            }
            result["items"] = cleanedList
        } else {
            throw new Exception("Missing 'items' key in input")
        }

        return result
    }

    private Map toChip(Map json, Map lensGroupsChips) {
        Map itemsToKeep = [:]
        json.each { key, value ->
            itemsToKeep[key] = walkThroughData(value, lensGroupsChips, true)
        }
        return itemsToKeep
    }


    private Map removeProperties(Map jsonMap, Map lensGroups, boolean goRecursive=false) {
        Map itemsToKeep = [:]
        Map types = lensGroups.get("lenses")
        Map showPropertiesField = types.get(jsonMap.get("@type"))
        if (jsonMap.get("@type") && types.get(jsonMap.get("@type").toString())) {
            def propertiesToKeep = showPropertiesField.get("showProperties")
            jsonMap.each {key, value ->
                if (key.toString() in propertiesToKeep || key.toString().startsWith("@")) {
                    if (goRecursive) {
                        itemsToKeep[key] = walkThroughData(value, lensGroups)
                    } else {
                        itemsToKeep[key] = value
                    }
                }
            }
            return itemsToKeep
        } else {
            return jsonMap
        }

    }

    private Object walkThroughData(Object o, Map displayData, boolean goRecursive) {
        if(o instanceof Map) {
            return removeProperties(o, displayData, goRecursive)
        } else if (o instanceof List){
            return walkThroughDataFromList(o, displayData, goRecursive)
        } else {
            return o
        }
    }


    private List walkThroughDataFromList(List items, Map displayData, boolean goRecursive) {
        List result = []
        items.each { item ->
            result << walkThroughData(item, displayData, goRecursive)
        }
        return result
    }


    /**
     * Given a list of IDs, return a map of JSON-LD found in storage.
     *
     * Map is of the form ID -> JSON-LD Map.
     *
     */
    Map getDocuments(List ids) {
        Map result = [:]
        ids.each { id ->
            if (id.startsWith(Document.BASE_URI.toString())) {
                id = Document.BASE_URI.resolve(id).getPath().substring(1)
            }
            // FIXME maybe we want to do a locate rathern than a load here?
            Document doc = whelk.storage.load(id)
            if (doc && !doc.deleted) {
                result[id] = doc.data
            }
        }
        return result
    }

    /**
     * Format and send GET response to client.
     *
     * Sets the necessary headers and picks the best Content-Type to use.
     *
     */
    void sendGetResponse(HttpServletRequest request,
                         HttpServletResponse response,
                         Document document, String path) {
        if (document && document.deleted) {
            response.sendError(HttpServletResponse.SC_GONE,
                               "Document has been deleted.")
            return
        }

        if (document && !document.deleted) {
            // FIXME remove?
            String ctheader = contextHeaders.get(path.split("/")[1])
            if (ctheader) {
                response.setHeader("Link",
                                   "<$ctheader>; " +
                                   "rel=\"http://www.w3.org/ns/json-ld#context\"; " +
                                   "type=\"application/ld+json\"")
            }

            response.setHeader("ETag", document.getModified())

            String contentType = CrudUtils.getBestContentType(request)

            if (path in contextHeaders.collect { it.value }) {
                log.debug("request is for context file. " +
                          "Must serve original content-type ($contentType).")
                // FIXME what should happen here?
                // contentType = document.contentType
            }

            Object responseBody = formatDocument(document, contentType)
            sendResponse(response, responseBody, contentType)
        } else {
            // FIXME move document null handling up?
            log.debug("Failed to find a document with URI $path")
            response.sendError(HttpServletRepsonse.SC_NOT_FOUND)
        }
    }

    /**
     * Format document based on Content-Type.
     *
     */
    Object formatDocument(Document document, String contentType) {
        if (contentType == MimeTypes.JSONLD) {
            Map result = document.data
            return result
        } else if (contentType == MimeTypes.RDF) {
            // FIXME convert to RDF-XML
            throw new UnsupportedContentTypeException("Not implemented.")
        } else if (contentType == MimeTypes.TURTLE) {
            // FIXME convert to Turtle
            throw new UnsupportedContentTypeException("Not implemented.")
        } else {
            throw new UnsupportedContentTypeException("Not implemented.")
        }
    }


    Document convertDocumentToAcceptedMediaType(Document document, String path, String acceptHeader, HttpServletResponse response) {

        List<String> accepting = acceptHeader?.split(",").collect {
            int last = (it.indexOf(';') == -1 ? it.length() : it.indexOf(';'))
            it.substring(0,last)
        }
        log.debug("Accepting $accepting")

        String extensionContentType = (mimeTypes.getContentType(path) == "application/octet-stream" ? null : mimeTypes.getContentType(path))
        log.debug("mimetype: $extensionContentType")
        if (!document && path ==~ /(.*\.\w+)/) {
            log.debug("Found extension in $path")
            if (!document && extensionContentType) {
                document = whelk.storage.load(path.substring(1, path.lastIndexOf(".")))
            }
            accepting = [extensionContentType]
        }

        if (document && accepting && !accepting.contains("*/*") && !accepting.contains(document.contentType) && !accepting.contains(getMajorContentType(document.contentType))) {
            FormatConverter fc = pico.getComponents(FormatConverter.class).find { accepting.contains(it.resultContentType) && it.requiredContentType == document.contentType }
            if (fc) {
                log.debug("Found formatconverter for ${fc.resultContentType}")
                document = fc.convert(document)
                if (extensionContentType) {
                    response.setHeader("Content-Location", path)
                }
            } else {
                document = null
                throw new UnsupportedContentTypeException("No supported types found in $accepting")
            }
        }
        return document
    }

    /**
     * Send 302 Found response
     *
     */
    void sendRedirect(HttpServletRequest request,
                      HttpServletResponse response, Location location) {
        if (location.getUri().getScheme() == null) {
            def locationRef = request.getScheme() + "://" +
                request.getServerName() +
                (request.getServerPort() != 80 ? ":" + request.getServerPort() : "") +
                request.getContextPath()
            response.setHeader("Location", locationRef + location.uri.toString())
        } else {
            response.setHeader("Location", location.uri.toString())
        }

        sendResponse(response, new byte[0], null, HttpServletResponse.SC_FOUND)
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling POST request.")

        if(request.pathInfo != "/") {
            log.debug("Invalid POST request URL.")
            response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                               "Method not allowed.")
            return
        }

        if (!isSupportedContentType(request)) {
            log.debug("Unsupported Content-Type for POST.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "Content-Type not supported.")
            return
        }

        Map requestBody = getRequestBody(request)

        if (isEmptyInput(requestBody)) {
            log.debug("Empty POST request.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "No data received")
            return
        }

        if (!JsonLd.isFlat(requestBody)) {
            log.debug("POST body is not flat JSON-LD")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "Body is not flat JSON-LD.")
        }

        // FIXME we're assuming Content-Type application/ld+json here
        // should we deny the others?
        String fullDocumentId = JsonLd.findFullIdentifier(requestBody)
        String documentId = JsonLd.findIdentifier(requestBody)

        // FIXME handle this better
        if (fullDocumentId &&
            fullDocumentId.startsWith(Document.BASE_URI.toString())) {
            log.debug("Invalid supplied ID in POST request.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "Supplied document ID not allowed.")
            return
        }

        Document existingDoc = whelk.storage.locate(documentId, true)?.document
        if (existingDoc) {
            log.debug("Tried to POST existing document.")
            response.sendError(HttpServletResponse.SC_CONFLICT,
                               "Document with ID ${documentId} already exists.")
            return
        }

        Document newDoc = new Document(requestBody)

        // if no identifier, create one
        if (!documentId) {
            newDoc.setId(mintIdentifier(requestBody))
        } else {
            // FIXME why does the caller need to set the ID?
            newDoc.setId(documentId)
        }

        // verify user permissions
        log.debug("Checking permissions for ${newDoc}")
        try {
            // TODO: 'collection' must also match the collection 'existingDoc'
            // is in.
            boolean allowed = hasPermission(request.getAttribute("user"),
                                            newDoc, existingDoc)
            if (!allowed) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN,
                                   "You are not authorized to perform this " +
                                   "operation")
                return
            }
        } catch (ModelValidationException mve) {
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               mve.getMessage())
            return
        }

        // try store document
        // return 201 or error
        String collection = request.getParameter("collection")
        boolean isUpdate = false
        Document savedDoc = saveDocument(newDoc, collection, isUpdate)
        sendCreateResponse(response, savedDoc.getURI().toString(),
                           savedDoc.getModified() as String)
    }

    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling PUT request.")

        if(request.pathInfo == "/") {
            log.debug("Invalid PUT request URL.")
            response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                               "Method not allowed.")
            return
        }

        if (!isSupportedContentType(request)) {
            log.debug("Unsupported Content-Type for PUT.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "Content-Type not supported.")
            return
        }

        Map requestBody = getRequestBody(request)

        if (isEmptyInput(requestBody)) {
            log.debug("Empty PUT request.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "No data received")
            return
        }

        String documentId = JsonLd.findIdentifier(requestBody)
        String idFromUrl = request.pathInfo.substring(1)

        if (!documentId) {
            log.debug("Missing document ID in PUT request.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "Missing @id in request.")
            return
        } else if (idFromUrl != documentId) {
            log.debug("Document ID does not match ID in URL.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "ID in document does not match ID in URL.")
            return
        }

        // FIXME don't use locate && handle alternate IDs
        Location location = whelk.storage.locate(documentId, true)
        Document existingDoc = location?.document
        if (!existingDoc && location?.uri) {
            response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                               "PUT does not support alternate IDs.")
            return
        } else if (!existingDoc) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                               "Document not found.")
            return
        } else {
            // FIXME not needed? should be handled by 303 See Other
            log.debug("Identifier was ${documentId}. Setting to ${existingDoc.id}")
            documentId = existingDoc.id
        }

        if (request.getHeader("If-Match") &&
            existingDoc.modified as String != request.getHeader("If-Match")) {
            log.debug("PUT performed on stale document.")
            response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED,
                               "The resource has been updated by someone " +
                               "else. Please refetch.")
            return
        }

        Document updatedDoc = new Document(requestBody)
        updatedDoc.setId(documentId)

        log.debug("Checking permissions for ${updatedDoc}")
        try {
            // TODO: 'collection' must also match the collection 'existingDoc'
            // is in.
            boolean allowed = hasPermission(request.getAttribute("user"),
                                            updatedDoc, existingDoc)
            if (!allowed) {
              response.sendError(HttpServletResponse.SC_FORBIDDEN,
                                 "You are not authorized to perform this " +
                                 "operation")
              return
            }
        } catch (ModelValidationException mve) {
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               mve.getMessage())
            return
        }
        log.debug("All checks passed.")

        String collection = request.getParameter("collection")
        boolean isUpdate = true
        Document savedDoc = saveDocument(updatedDoc, collection, isUpdate)
        sendUpdateResponse(response, savedDoc.getURI().toString(),
                           savedDoc.getModified() as String)

    }

    boolean isEmptyInput(Map inputData) {
        if (inputData.size() == 0) {
            return true
        } else {
            return false
        }
    }

    boolean isSupportedContentType(HttpServletRequest request) {
        ContentType contentType = ContentType.parse(request.getContentType())
        String mimeType = contentType.getMimeType()
        // FIXME add additional content types?
        if (mimeType == "application/ld+json") {
            return true
        } else {
            return false
        }
    }

    Map getRequestBody(HttpServletRequest request) {
        byte[] body = request.getInputStream().getBytes()

        try {
            return mapper.readValue(body, Map)
        } catch (EOFException) {
            return [:]
        }
    }

    Document saveDocument(Document doc, String collection, boolean isUpdate) {
        try {
            if (doc) {
                log.debug("Saving document (${doc.getShortId()})")
                log.info("Document accepted: created is: ${doc.getCreated()}")

                doc = whelk.store(doc, "xl", null, collection, false, isUpdate)

                return doc
            }
        } catch (StorageCreateFailedException scfe) {
            log.warn("Already have document with id ${scfe.duplicateId}")
            response.sendError(HttpServletResponse.SC_CONFLICT,
                               "Document with id \"${scfe.duplicateId}\" " +
                               "already exists.")
        } catch (WhelkAddException wae) {
            log.warn("Whelk failed to store document: ${wae.message}")
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE ,
                               wae.message)
        } catch (Exception e) {
            log.error("Operation failed", e)
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                               e.message)
        }
    }

    void sendCreateResponse(HttpServletResponse response, String locationRef,
                            String etag) {
        sendDocumentSavedResponse(response, locationRef, etag, true)
    }

    void sendUpdateResponse(HttpServletResponse response, String locationRef,
                            String etag) {
        sendDocumentSavedResponse(response, locationRef, etag, false)
    }

    void sendDocumentSavedResponse(HttpServletResponse response,
                                   String locationRef, String etag,
                                   boolean newDocument) {
        log.debug("Setting header Location: $locationRef")

        response.setHeader("Location", locationRef)
        response.setHeader("ETag", etag as String)

        if (newDocument) {
            response.setStatus(HttpServletResponse.SC_CREATED)
        } else {
            response.setStatus(HttpServletResponse.SC_NO_CONTENT)
        }
    }


    boolean hasPermission(userInfo, newdoc, olddoc) {
        if (userInfo) {
            log.debug("User is: $userInfo")
            if (userInfo.user == "SYSTEM") {
                log.warn("User is SYSTEM. Allowing access to all.")
                return true
            }
            return accessControl.checkDocument(newdoc, olddoc, userInfo)
        }
        log.info("No user information received, denying request.")
        return false
    }

    @Deprecated
    List<String> getAlternateIdentifiersFromLinkHeaders(HttpServletRequest request) {
        def alts = []
        for (link in request.getHeaders("Link")) {
            def (id, rel) = link.split(";")*.trim()
            if (rel.replaceAll(/"/, "") == "rel=${SAMEAS_NAMESPACE}") {
                def match = id =~ /<([\S]+)>/
                if (match.matches()) {
                    alts << match[0][1]
                }
            }
        }
        return alts
    }

    String mintIdentifier(Map data) {
        String id = null
        if (data) {
            id = JsonLd.findIdentifier(data)
        }
        return id ?: IdGenerator.generate()
    }


    @Override
    void doDelete(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling DELETE request.")
        if(request.pathInfo == "/") {
            response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed.")
            return
        }
        try {
            String id = request.pathInfo.substring(1)
            def doc = whelk.storage.load(id)
            if(!doc) {
                response.sendError(HttpServletResponse.SC_NOT_FOUND, "Document not found.")
            } else if (doc && !hasPermission(request.getAttribute("user"), null, doc)) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN, "You do not have sufficient privileges to perform this operation.")
            } else {
                log.debug("Removing resource at ${id}")
                // FIXME don't hardcode collection
                whelk.remove(id, "xl", null, "xl")
                response.setStatus(HttpServletResponse.SC_NO_CONTENT)
            }
        } catch (Exception wre) {
            log.error("Something went wrong", wre)
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, wre.message)
        }

    }



}

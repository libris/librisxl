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
import whelk.converter.FormatConverter
import whelk.converter.marc.JsonLD2MarcConverter
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.exception.InvalidQueryException
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
 */
@Log
class Crud extends HttpServlet {

    final static String SAMEAS_NAMESPACE = "http://www.w3.org/2002/07/owl#sameAs"
    final static String DOCBASE_URI = "http://libris.kb.se/" // TODO: encapsulate and configure (LXL-260)

    enum FormattingType {
        FRAMED, EMBELLISHED, FRAMED_AND_EMBELLISHED, RAW
    }

    static final JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter();

    MimetypesFileTypeMap mimeTypes = new MimetypesFileTypeMap()

    final static Map contextHeaders = [
            "bib": "/sys/context/lib.jsonld",
            "auth": "/sys/context/lib.jsonld",
            "hold": "/sys/context/lib.jsonld"
    ]
    Whelk whelk

    String vocabUri = "https://id.kb.se/vocab/" // TODO: encapsulate and configure (LXL-260)
    Map vocabData
    String vocabDisplayUri = "https://id.kb.se/vocab/display" // TODO: encapsulate and configure (LXL-260)
    Map displayData

    SearchUtils search
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
        displayData = whelk.storage.locate(vocabDisplayUri, true).document.data
        vocabData = whelk.storage.locate(vocabUri, true).document.data
        search = new SearchUtils(whelk, displayData, vocabData)

    }

    void handleQuery(HttpServletRequest request, HttpServletResponse response,
                     String dataset, String siteBaseUri = null) {
        Map queryParameters = new HashMap<String, String[]>(request.getParameterMap())
        String callback = queryParameters.remove("callback")

        try {
            Map results = search.doSearch(queryParameters, dataset, siteBaseUri)
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
        } catch (InvalidQueryException iqe) {
            log.error("Invalid query: ${queryParameters}")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "Invalid query, please check the documentation.")
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

        if (request.pathInfo == "/find" || request.pathInfo == "/find.json") {
            String collection = request.getParameter("collection")
            handleQuery(request, response, collection)
            return
        }

        def marcframePath = "/sys/marcframe.json"
        if (request.pathInfo == marcframePath) {
            def responseBody = getClass().classLoader.getResourceAsStream("ext/marcframe.json").getText("utf-8")
            sendGetResponse(request, response, responseBody, "1970/1/1", marcframePath, "application/json")
            return
        }

        try {
            def path = request.pathInfo

            // Tomcat incorrectly strips away double slashes from the pathinfo.
            // Compensate here.
            if (path ==~ "/http:/[^/].+") {
                path = path.replace("http:/", "http://")
            } else if (path ==~ "/https:/[^/].+") {
                path = path.replace("https:/", "https://")
            }

            handleGetRequest(request, response, path)
        } catch (UnsupportedContentTypeException ucte) {
            response.sendError(response.SC_NOT_ACCEPTABLE, ucte.message)
        } catch (WhelkRuntimeException wrte) {
            response.sendError(response.SC_INTERNAL_SERVER_ERROR, wrte.message)
        }
    }

    void handleGetRequest(HttpServletRequest request,
                          HttpServletResponse response,
                          String path) {
        String id = getIdFromPath(path)
        String version = request.getParameter("version")

        // TODO: return already loaded displayData and vocabData (cached on modified)? (LXL-260)
        Tuple2 docAndLocation = getDocumentFromStorage(id, version)
        Document doc = docAndLocation.first
        Location loc = docAndLocation.second

        if (!doc && !loc) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                               "Document not found.")
            return
        } else if (!doc && loc) {
            sendRedirect(request, response, loc)
            return
        } else if (doc && doc.deleted) {
            response.sendError(HttpServletResponse.SC_GONE,
                               "Document has been deleted.")
            return
        } else {
            String contentType = CrudUtils.getBestContentType(request)
            def responseBody = getFormattedResponseBody(doc, path, contentType)
            String modified = doc.getModified()
            sendGetResponse(request, response, responseBody, modified,
                            path, contentType)
            return
        }
    }

    private Object getFormattedResponseBody(Document doc, String path,
                                            String contentType) {
        FormattingType format = getFormattingType(path, contentType)
        log.debug("Formatting document ${doc.getCompleteId()} with format " +
                  "${format} and content type ${contentType}")
        def result
        switch (format) {
            case FormattingType.RAW:
                result = doc.data
                break
            case FormattingType.EMBELLISHED:
                doc = getEmbellishedDocument(doc)
                result = doc.data
                break
            case FormattingType.FRAMED:
                result = JsonLd.frame(doc.getCompleteId(), doc.data)
                break
            case FormattingType.FRAMED_AND_EMBELLISHED:
                doc = getEmbellishedDocument(doc)
                result = JsonLd.frame(doc.getCompleteId(), doc.data)
                break
            default:
                throw new WhelkRuntimeException("Invalid formatting type: ${format}")
        }

        return formatResponseBody(result, contentType)
    }

    private Document getEmbellishedDocument(Document doc) {
        List externalRefs = doc.getExternalRefs()
        List convertedExternalLinks = convertExternalLinks(externalRefs)
        Map referencedDocuments = getDocuments(convertedExternalLinks)
        doc.embellish(referencedDocuments, displayData)

        return doc
    }

    /**
     * Format response body based on Content-Type.
     *
     */
    Object formatResponseBody(Object responseBody, String contentType) {
        if (contentType == MimeTypes.JSONLD) {
            return responseBody
        } else if (contentType == MimeTypes.JSON) {
            return responseBody
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

    private List convertExternalLinks(List refs) {
        List result = []
        refs.each { ref ->
            def match
            if ((match = ref =~ /^([a-z0-9]+):(.*)$/)) {
                def resolved = this.displayData['@context'][match[0][1]]
                if (resolved) {
                    URI base = new URI(resolved)
                    result << base.resolve(match[0][2]).toString()
                }
            } else {
                result << ref
            }
        }
        return result
    }

    /**
     * Given a resource path, return the ID part or null.
     *
     * E.g.:
     * getIdFromPath("/foo") -> "foo"
     * getIdFromPath("/foo/data.jsonld") -> "foo"
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
     * Given a resource path and a content type, return the formatting
     * type for that resource.
     *
     * FormattingType.EMBELLISHED is the default return value.
     *
     */
    static FormattingType getFormattingType(String path, String contentType) {
        log.debug("Getting formatting type for path ${path}")
        int viewGroup = 3
        int fileEndingGroup = 5
        def pattern = getPathRegex()
        def matcher = path =~ pattern
        if (matcher.matches()) {
            String view = matcher.group(viewGroup)
            String fileEnding = matcher.group(fileEndingGroup)
            return getFormattingTypeForView(view, fileEnding, contentType)
        } else {
            return FormattingType.EMBELLISHED
        }
    }

    private static FormattingType getFormattingTypeForView(String view,
                                                           String fileEnding,
                                                           String contentType) {
        FormattingType result

        if (!view) {
            result = getFormattingTypeForFancyView(contentType)
        } else if (view == 'data') {
            switch (fileEnding) {
                case 'jsonld':
                    result = FormattingType.RAW
                    break
                case 'json':
                    result = FormattingType.FRAMED
                    break
                case null:
                    result = getFormattingTypeForSimpleView(contentType)
                    break
                default:
                    // TODO support more file types
                    throw new WhelkRuntimeException("Bad file ending: " +
                                                    "${fileEnding}")
            }
        } else if (view == 'data-view') {
            switch (fileEnding) {
                case 'jsonld':
                    result = FormattingType.EMBELLISHED
                    break
                case 'json':
                    result = FormattingType.FRAMED_AND_EMBELLISHED
                    break
                case null:
                    result = getFormattingTypeForFancyView(contentType)
                    break
                default:
                    // TODO support more file types
                    throw new WhelkRuntimeException("Bad file ending: ${fileEnding}")
            }
        } else {
            return FormattingType.EMBELLISHED
        }

        return result
    }

    private static FormattingType getFormattingTypeForFancyView(String contentType) {
        FormattingType result

        switch (contentType) {
            case MimeTypes.JSONLD:
                result = FormattingType.EMBELLISHED
                break
            case MimeTypes.JSON:
                result = FormattingType.FRAMED_AND_EMBELLISHED
                break
            default:
                result = FormattingType.EMBELLISHED
                break
        }

        return result
    }

    private static FormattingType getFormattingTypeForSimpleView(String contentType) {
        FormattingType result

        switch (contentType) {
            case MimeTypes.JSONLD:
                result = FormattingType.RAW
                break
            case MimeTypes.JSON:
                result = FormattingType.FRAMED
                break
            default:
                result = FormattingType.RAW
                break
        }

        return result
    }

    /**
     * Return a regex pattern representation of a CRUD path.
     *
     * Matches /foo, /foo/data, /foo/data.<suffix>.
     *
     */
    static String getPathRegex() {
        return ~/^\/(.+?)(\/(data|data-view)(\.(\w+))?)?$/
    }

    /**
     * Get (Document, Location) from storage for specified ID and version.
     *
     * If version is null, we look for the latest version.
     * Document and Location in the response may be null.
     *
     */
    Tuple2 getDocumentFromStorage(String id, String version=null) {
        if (version) {
            Document doc = whelk.storage.load(id, version)
            return new Tuple2(doc, null)
        } else {
            Location location = whelk.storage.locate(id, true)
            Document document = location?.document
            if (!document && location?.uri) {
                return new Tuple2(null, location)
            } else if (!document) {
                return new Tuple2(null, null)
            } else {
                return new Tuple2(document, location)
            }
        }
    }

    /**
     * Given a list of IDs, return a map of JSON-LD found in storage.
     *
     * Map is of the form ID -> JSON-LD Map.
     *
     */
    Map getDocuments(List ids) {
        Map result = [:]
        Document doc
        ids.each { id ->
            if (id.startsWith(Document.BASE_URI.toString())) {
                id = Document.BASE_URI.resolve(id).getPath().substring(1)
                doc = whelk.storage.load(id)
            } else {
                doc = whelk.storage.locate(id, true)?.document
            }

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
                         Object responseBody, String modified, String path,
                         String contentType) {
        // FIXME remove?
        String ctheader = contextHeaders.get(path.split("/")[1])
        if (ctheader) {
            response.setHeader("Link",
                               "<$ctheader>; " +
                               "rel=\"http://www.w3.org/ns/json-ld#context\"; " +
                               "type=\"application/ld+json\"")
        }

        response.setHeader("ETag", modified)

        if (path in contextHeaders.collect { it.value }) {
            log.debug("request is for context file. " +
                      "Must serve original content-type ($contentType).")
            // FIXME what should happen here?
            // contentType = document.contentType
        }

        sendResponse(response, responseBody, contentType)
    }

    Document convertDocumentToAcceptedMediaType(Document document, String path, String acceptHeader, HttpServletResponse response) {

        List<String> accepting = acceptHeader?.split(",").collect {
            int last = (it.indexOf(';') == -1 ? it.length() : it.indexOf(';'))
            it.substring(0, last)
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
        String redirUrl = location.uri.toString()
        if (location.getUri().getScheme() == null) {
            def locationRef = request.getScheme() + "://" +
                request.getServerName() +
                (request.getServerPort() != 80 ? ":" + request.getServerPort() : "") +
                request.getContextPath()
            redirUrl = locationRef + location.uri.toString()
        }
        response.setHeader("Location", redirUrl)
        log.debug("Redirecting to document location: ${redirUrl}")
        sendResponse(response, new byte[0], null, HttpServletResponse.SC_FOUND)
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling POST request.")

        if (request.pathInfo != "/") {
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
            boolean allowed = hasPostPermission(newDoc, request.getAttribute("user"))
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

        if (request.pathInfo == "/") {
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
            boolean allowed = hasPutPermission(updatedDoc, existingDoc,
                                               request.getAttribute("user"))
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
        if (!inputData || inputData.size() == 0) {
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
            response.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE,
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

    boolean hasPostPermission(Document newDoc, Map userInfo) {
        if (userInfo) {
            log.debug("User is: $userInfo")
            if (isSystemUser(userInfo)) {
                return true
            } else {
                return accessControl.checkDocumentToPost(newDoc, userInfo)
            }
        }
        log.info("No user information received, denying request.")
        return false
    }

    boolean hasPutPermission(Document newDoc, Document oldDoc, Map userInfo) {
        if (userInfo) {
            log.debug("User is: $userInfo")
            if (isSystemUser(userInfo)) {
                return true
            } else {
                return accessControl.checkDocumentToPut(newDoc, oldDoc, userInfo)
            }
        }
        log.info("No user information received, denying request.")
        return false
    }

    boolean hasDeletePermission(Document oldDoc, Map userInfo) {
        if (userInfo) {
            log.debug("User is: $userInfo")
            if (isSystemUser(userInfo)) {
                return true
            } else {
                return accessControl.checkDocumentToDelete(oldDoc, userInfo)
            }
        }
        log.info("No user information received, denying request.")
        return false
    }

    boolean isSystemUser(Map userInfo) {
        if (userInfo.user == "SYSTEM") {
            log.warn("User is SYSTEM. Allowing access to all.")
            return true
        }
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
        if (request.pathInfo == "/") {
            response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed.")
            return
        }
        try {
            String id = request.pathInfo.substring(1)
            def doc = whelk.storage.load(id)

            log.debug("Checking permissions for ${doc}")

            if (!doc) {
                Location loc = whelk.storage.locate(id, true)
                if (loc) {
                    sendRedirect(request, response, loc)
                } else {
                    response.sendError(HttpServletResponse.SC_NOT_FOUND, "Document not found.")
                }
            } else if (doc && !hasDeletePermission(doc, request.getAttribute("user"))) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN, "You do not have sufficient privileges to perform this operation.")
            } else {
                log.debug("Removing resource at ${id}")
                // FIXME don't hardcode collection
                whelk.remove(id, "xl", null, "xl")
                response.setStatus(HttpServletResponse.SC_NO_CONTENT)
            }
        } catch (ModelValidationException mve) {
            // FIXME data leak
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    mve.getMessage())
        } catch (Exception wre) {
            log.error("Something went wrong", wre)
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, wre.message)
        }

    }

}

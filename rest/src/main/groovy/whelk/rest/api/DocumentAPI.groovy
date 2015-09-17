package whelk.rest.api

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*
import javax.activation.MimetypesFileTypeMap

import org.codehaus.jackson.map.ObjectMapper
import org.apache.http.entity.ContentType

import whelk.*
import whelk.converter.*
import whelk.exception.*
import whelk.rest.*
import whelk.rest.security.*

import java.util.regex.Pattern

@Log
class DocumentAPI implements RestAPI {
    MimetypesFileTypeMap mt = new MimetypesFileTypeMap()

    final static String SAMEAS_NAMESPACE = "http://www.w3.org/2002/07/owl#sameAs"
    final static String DOCBASE_URI = "http://libris.kb.se/"

    final static ObjectMapper mapper = new ObjectMapper()

    String description = "A GET request with identifier loads a document. A PUT request stores a document. A DELETE request deletes a document."

    private Whelk whelk

    Map contextHeaders = [:]
    AccessControl accessControl = new AccessControl()

    Pattern pathPattern = Pattern.compile("^([^_].*)\$")

    DocumentAPI(Whelk w) {
        whelk = w
        log.info("Doc api instantiated.")
    }

    def determineDisplayMode(path) {
        if (path.endsWith("/meta")) {
            return [path[0 .. -6], DisplayMode.META]
        }
        if (path.endsWith("/_raw")) {
            return [path[0 .. -6], DisplayMode.RAW]
        }
        return [path, DisplayMode.DOCUMENT]
    }

    String getCleanPath(List pathVars) {
        if (pathVars) {
            //return "/"+pathVars.first().replaceAll('\\/\\/', '/')
            if (pathVars.first().startsWith("/")) {
                return pathVars.first()
            } else {
                return "/"+pathVars.first()
            }
        }
        return "/"
    }

    void handle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        String path = pathVars.first()
        log.debug "Path: $path req method: ${request.method} req url: ${request.requestURL} forward req uri: ${request.getAttribute('javax.servlet.forward.request_uri')}"
        if (request.method == "GET" && path.startsWith("/_iri/")) {
            def docBase = new StringBuilder(DOCBASE_URI)
            while (docBase[-1] == '/') {
                docBase.deleteCharAt(docBase.length()-1)
            }
            def iri = path - docBase.toString()
            handleIriRequest(request, response, iri)
        } else if (request.method == "GET") {
            handleGetRequest(request, response, path)
        } else if (request.method == "POST") {
            log.debug("POST detected.")
            int pathsize = path.split("/").size()
            if (pathsize == 0 || pathsize == 2) {
                handlePutAndPostRequest(request, response, path, false)
            } else {
                response.sendError(response.SC_BAD_REQUEST, "POST only allowed to root or dataset")
            }
        } else if (request.getMethod() == "PUT") {
            handlePutAndPostRequest(request, response, path, true)
        } else if (request.method == "DELETE") {
            try {
                def doc = whelk.storage.load(path)
                if (doc && !hasPermission(request.getAttribute("user"), doc, null)) {
                    response.setStatus(HttpServletResponse.SC_FORBIDDEN, "You do not have sufficient privileges to perform this operation.")
                    return
                } else {
                    log.debug("Removing resource at $path")
                    whelk.remove(path)
                    response.setStatus(HttpServletResponse.SC_NO_CONTENT)

                }
            } catch (WhelkRuntimeException wre) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, wre.message)
            }
        } else {
            response.sendError(response.SC_FORBIDDEN, "Inappropriate request.")
        }
    }

    void handleIriRequest(HttpServletRequest request, HttpServletResponse response, String iriPath) {
        log.info("identifier: $iriPath")

        def location = whelk.storage.locate(iriPath)
        def doc = location?.document
        log.info("location: $location, doc: $doc")
        def locationRef = request.getScheme() + "://" + request.getServerName() + (request.getServerPort() != 80 ? ":" + request.getServerPort() : "") + request.getContextPath()
        def locationId = doc?.identifier ?: location?.uri
        log.info("locationId: $locationId")
        if (locationId) {
            response.setHeader("Location", locationRef + locationId)
            sendResponse(response, null, null, 303)
            return
        }
        log.info("Not found!")
        response.setStatus(HttpServletResponse.SC_NOT_FOUND)

    }

    void handleQuery(HttpServletRequest request, HttpServletResponse response, String path) {
        log.debug("Handle query for ds " + getDatasetBasedOnPath(path))
        def query = new LinkedDataAPIQuery(request.parameterMap)
        query.setIndexTypes(getDatasetBasedOnPath(path))
        log.trace("query: " + query.toJsonQuery())
        def callback = request.getParameter("callback")
        def jsonResult = (callback ? callback + "(" : "") + this.whelk.search(query).toJson() + (callback ? ");" : "")
        sendResponse(response, jsonResult, "application/json")
    }

    void handleGetRequest(HttpServletRequest request, HttpServletResponse response, String path) {
        if (path.endsWith("/")) {
            handleQuery(request, response, path)
            return
        }
        def mode = DisplayMode.DOCUMENT
        (path, mode) = determineDisplayMode(path)
        def version = request.getParameter("version")
        boolean flat = request.getParameter("flat") == "true"
        def accepting = request.getHeader("accept")?.split(",").collect {
            int last = (it.indexOf(';') == -1 ? it.length() : it.indexOf(';'))
            it.substring(0,last)
        }
        log.debug("Accepting $accepting")
        try {
            def d = null
            if (version) {
                d = whelk.storage.load(path, version)
            } else {
                def location = whelk.storage.locate(path)
                d = location?.document
                if (!d && location?.uri) {
                    def locationRef = request.getScheme() + "://" + request.getServerName() + (request.getServerPort() != 80 ? ":" + request.getServerPort() : "") + request.getContextPath()
                    response.setHeader("Location", locationRef + location.uri.toString())
                    sendResponse(response, new byte[0], null, location.responseCode)
                    return
                }
            }
            def extensionContentType = (mt.getContentType(path) == "application/octet-stream" ? null : mt.getContentType(path))
            log.debug("mimetype: $extensionContentType")
            if (!d && path ==~ /(.*\.\w+)/) {
                log.debug("Found extension in $path")
                if (!d && extensionContentType) {
                    d = whelk.storage.load(path.substring(0, path.lastIndexOf(".")))
                }
                accepting = [extensionContentType]
            }
            if (d && accepting && !accepting.contains("*/*") && !accepting.contains(d.contentType) && !accepting.contains(getMajorContentType(d.contentType))) {
                def fc = plugins.find { it instanceof FormatConverter && accepting.contains(it.resultContentType) && it.requiredContentType == d.contentType }
                if (fc) {
                    log.debug("Found formatconverter for ${fc.resultContentType}")
                    d = fc.convert(d)
                    if (extensionContentType) {
                        response.setHeader("Content-Location", path)
                    }
                } else {
                    d = null
                }
            }

            if (d && (mode== DisplayMode.META || !d.manifest['deleted'])) {

                if (mode == DisplayMode.META) {
                    def versions = whelk.storage.loadAllVersions(d.identifier)
                    if (versions) {
                        d.manifest.versions = versions
                    }
                    sendResponse(response, d.getManifestAsJson(), "application/json")
                } else {
                    def ctheader = contextHeaders.get(path.split("/")[1])
                    if (ctheader) {
                        response.setHeader("Link", "<$ctheader>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"")
                    }
                    response.setHeader("ETag", d.modified as String)
                    def contentType = getMajorContentType(d.contentType)
                    if (path in contextHeaders.collect { it.value })  {
                        contentType = d.contentType
                        log.debug("request is for context file. Must serve original content-type ($contentType).")
                    }
                    if (flat) {
                        sendResponse(response, JsonLd.flatten(d.data), contentType)
                    } else {
                        log.info("Framing ${d.identifier} ...")
                        sendResponse(response, JsonLd.frame(d.identifier, d.data), contentType)
                    }
                }
            } else {
                log.debug("Failed to find a document with URI $path")
                response.sendError(response.SC_NOT_FOUND)
            }
        } catch (WhelkRuntimeException wrte) {
            response.sendError(response.SC_INTERNAL_SERVER_ERROR, wrte.message)
        }
    }

    void handlePutAndPostRequest(HttpServletRequest request, HttpServletResponse response, String path, boolean identifierSupplied) {
        log.debug("PATH: $path")
        try {
            if (path == "/") {
                throw new WhelkRuntimeException("PUT requires a proper URI.")
            }
            def manifest = [:]
            Document existingDoc = null

            if (identifierSupplied) {
                existingDoc = whelk.storage.load(path)
                if (existingDoc) {
                    log.debug("Document with identifier ${existingDoc.identifier} already exists.")
                    // Check If-Match
                    String ifMatch = request.getHeader("If-Match")
                    if (ifMatch && existingDoc.modified as String != ifMatch) {
                        response.sendError(response.SC_PRECONDITION_FAILED, "The resource has been updated by someone else. Please refetch.")
                        return
                    }
                    manifest = existingDoc.manifest
                    manifest.remove("deleted")
                }
                else {
                    manifest['identifier'] = path
                }
            }
            String cType = ContentType.parse(request.getContentType()).getMimeType()
            if (cType == "application/x-www-form-urlencoded") {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Content-Type application/x-www-form-urlencoded is not supported")
            }

            manifest["contentType"] = cType
            log.debug("Set ct: ${manifest.contentType}")
            manifest["dataset"] = getDatasetBasedOnPath(path)
            log.debug("Set dataset: ${manifest.dataset}")

            try {
                Document doc = new Document(mapper.readValue(request.getInputStream().getBytes(), Map), manifest)    // whelk.createDocument(manifest["contentType"]).withManifest(manifest).withData(request.getInputStream().getBytes())

                if (!hasPermission(request.getAttribute("user"), doc, existingDoc)) {
                    response.setStatus(HttpServletResponse.SC_FORBIDDEN, "You do not have sufficient privileges to perform this operation.")
                    return
                }

                //doc = whelk.sanityCheck(doc)

                getAlternateIdentifiersFromLinkHeaders(request).each {
                    doc.addIdentifier(it);
                }

                log.debug("Saving document (${doc.identifier})")
                def identifier = whelk.store(doc)

                def locationRef = request.getRequestURL()

                if (!identifierSupplied) {
                    while (locationRef[-1] == '/') {
                        locationRef.deleteCharAt(locationRef.length()-1)
                    }

                    if (manifest['dataset'] && locationRef.toString().endsWith(manifest['dataset'])) {
                        int endPos = locationRef.length()
                        int startPos = endPos - manifest['dataset'].length() - 1
                        locationRef.delete(startPos, endPos)
                    }

                    locationRef.append(identifier)

                }

                sendDocumentSavedResponse(response, locationRef.toString(), doc.modified as String)

            } catch (DocumentException de) {
                log.warn("Document exception: ${de.message}")
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, de.message)
            } catch (WhelkAddException wae) {
                log.warn("Whelk failed to store document: ${wae.message}")
                response.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE , wae.message)
            }
        } catch (WhelkRuntimeException wre) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST, wre.message)
        }
    }

    boolean hasPermission(info, newdoc, olddoc) {
        if (info) {
            log.debug("User is: $info")
            if (info.user == "SYSTEM") {
                return true
            }
            return accessControl.checkDocument(newdoc, olddoc, info)
        }
        log.info("No user information received, denying request.")
        return false
    }

    void sendDocumentSavedResponse(HttpServletResponse response, String locationRef, String etag) {
        log.debug("Setting header Location: $locationRef")
        response.setHeader("Location", locationRef)
        response.setHeader("ETag", etag as String)
        response.setStatus(HttpServletResponse.SC_CREATED, "Thank you! Document ingested.")
    }

    private String getDatasetBasedOnPath(path) {
        String ds = ""
        def elements = path.split("/")
        int i = 1
        while (ds.length() == 0) {
            ds = elements[i++]
        }
        log.trace("Estimated dataset: $ds")
        return ds
    }

    String getMajorContentType(String contentType) {
        return contentType?.replaceAll("/[\\w]+\\+", "/")
    }

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

    static void sendResponse(HttpServletResponse response, Map data, String contentType, int statusCode = 200) {
        if (!data) {
            sendResponse(response, new byte[0], contentType, statusCode)
        } else {
            sendResponse(response, mapper.writeValueAsBytes(data), contentType, statusCode)
        }
    }

    static void sendResponse(HttpServletResponse response, byte[] data, String contentType, int statusCode = 200) {
        response.setStatus(statusCode)
        if (contentType) {
            response.setContentType(contentType)
            if (contentType.startsWith("text/") || contentType.startsWith("application/")) {
                response.setCharacterEncoding("UTF-8")
            }
        }

        if (data.length > 0) {
            OutputStream out = response.getOutputStream()
            out.write(data, 0, data.length)
            //response.writer.write(text)
            //response.writer.flush()
            out.flush()
            out.close()
        }
    }


}

enum DisplayMode {
DOCUMENT, META, RAW
}

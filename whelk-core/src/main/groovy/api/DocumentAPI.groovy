package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*

import se.kb.libris.conch.Tools

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.exception.*

@Log
class DocumentAPI extends BasicAPI {
    String description = "A GET request with identifier loads a document. A PUT request stores a document. A DELETE request deletes a document."


    DocumentAPI(Map settings) {
    }

    def determineDisplayMode(path) {
        if (path.endsWith("/meta")) {
            return [path[0 .. -6], DisplayMode.META]
        }
        return [path, DisplayMode.DOCUMENT]
    }

    String getCleanPath(List pathVars) {
        if (pathVars) {
            return "/"+pathVars.first().replaceAll('\\/\\/', '/')
        }
        return "/"
    }

    protected void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        String path = getCleanPath(pathVars)
        log.debug "Path: $path"
        def mode = DisplayMode.DOCUMENT

        (path, mode) = determineDisplayMode(path)
        if (request.method == "GET") {
            log.debug "Request path: ${path}"
            log.debug " DisplayMode: $mode"
            def version = request.getParameter("version")
            def accepting = request.getHeader("accept")?.split(",").collect {
                    int last = (it.indexOf(';') == -1 ? it.length() : it.indexOf(';'))
                    it.substring(0,last)
                }

            log.debug("Accepting $accepting")
            try {
                log.debug("We want version $version")
                def d = whelk.get(new URI(path), version, accepting)
                log.debug("Document received from whelk: $d")
                if (d && (mode == DisplayMode.META || !d.entry['deleted'])) {
                    if (mode == DisplayMode.META) {
                        sendResponse(response, d.metadataAsJson, "application/json")
                    } else {
                        response.setHeader("ETag", d.timestamp as String)
                        sendResponse(response, d.dataAsString, d.contentType)
                    }
                } else {
                    log.debug("Failed to find a document with URI $path")
                    response.sendError(response.SC_NOT_FOUND)
                }
            } catch (WhelkRuntimeException wrte) {
                response.sendError(response.SC_INTERNAL_SERVER_ERROR, wrte.message)
            }
        } else if (request.method == "POST") {
            try {
                log.debug("request: $request")
                Document doc = new Document().withData(Tools.normalizeString(request.getInputStream().getText("UTF-8"))).withEntry(["contentType":request.getContentType()])
                doc = this.whelk.sanityCheck(doc)
                def identifier = this.whelk.add(doc)
                response.setHeader("ETag", doc.timestamp as String)
                /*
                sendResponse(response, doc.dataAsString, doc.contentType)
                */
                log.debug("Saved document $identifier")
                def locationRef = request.getRequestURL()
                while (locationRef[-1] == '/') {
                    locationRef.deleteCharAt(locationRef.length()-1)
                }
                locationRef.append(identifier)
                log.debug("Setting location for redirect: $locationRef")
                response.setHeader("Location", locationRef.toString())
                response.sendError(HttpServletResponse.SC_CREATED, "Thank you! Document ingested with id ${identifier}")
                /*
                response.setStatus(Status.REDIRECTION_SEE_OTHER, "Thank you! Document ingested with id ${identifier}")
                log.debug("Redirecting with location ref " + request.getRootRef().toString() + identifier)
                response.setLocationRef(request.getRootRef().toString() + "${identifier}")
                */
            } catch (WhelkRuntimeException wre) {
                response.sendError(response.SC_INTERNAL_SERVER_ERROR, wre.message)
            }
        } else if (request.getMethod() == "PUT") {
            log.info("PATH: $path")
            try {
                if (path == "/") {
                    throw new WhelkRuntimeException("PUT requires a proper URI.")
                }
                def identifier
                def entry = [
                    "identifier":path,
                    "contentType":request.getContentType(),
                    "dataset":getDatasetBasedOnPath(path)
                    ]
                def meta = null

                // Check If-Match
                String ifMatch = request.getHeader("If-Match")
                if (ifMatch
                    && this.whelk.get(new URI(path))
                    && this.whelk.get(new URI(path))?.timestamp as String != ifMatch) {
                    response.sendError(response.SC_PRECONDITION_FAILED, "The resource has been updated by someone else. Please refetch.")
                } else {
                    try {
                        Document doc = new Document(["entry":entry,"meta":meta]).withData(request.getInputStream().getBytes())
                        this.whelk.add(doc)
                        /*
                        identifier = this.whelk.add(
                            request.getInputStream().getBytes(),
                            entry,
                            meta
                            )
                        */
                        def locationRef = request.getRequestURL()
                        log.debug("Setting location for redirect: $locationRef")
                        response.setHeader("Location", locationRef.toString())
                        response.setHeader("ETag", doc.timestamp as String)
                        response.sendError(HttpServletResponse.SC_CREATED, "Thank you! Document ingested with id ${identifier}")
                    } catch (DocumentException de) {
                        log.warn("Document exception: ${de.message}")
                        response.setStatus(HttpServletResponse.SC_BAD_REQUEST, de.message)
                    } catch (WhelkAddException wae) {
                        log.warn("Whelk failed to store document: ${wae.message}")
                        response.setStatus(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE , wae.message)
                    }
                }
            } catch (WhelkRuntimeException wre) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST, wre.message)
            }
        }
        else if (request.method == "DELETE") {
            try {
                log.debug("Removing resource at $path")
                whelk.remove(new URI(path))
                response.setStatus(HttpServletResponse.SC_NO_CONTENT)
            } catch (WhelkRuntimeException wre) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, wre.message)
            }
        }
    }

    private String getDatasetBasedOnPath(path) {
        String ds = ""
        def elements = path.split("/")
        int i = 1
        while (ds.length() == 0 || ds == whelk.id) {
            ds = elements[i++]
        }
        return ds
    }
}

enum DisplayMode {
    DOCUMENT, META
}

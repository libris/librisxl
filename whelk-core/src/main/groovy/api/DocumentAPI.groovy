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
         Map contextHeaders = [:]
         DocumentAPI(Map settings) {
             this.contextHeaders = settings.get("contextHeaders", [:])
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
             def version = request.getParameter("version")
             def accepting = request.getHeader("accept")?.split(",").collect {
                 int last = (it.indexOf(';') == -1 ? it.length() : it.indexOf(';'))
                 it.substring(0,last)
             }
             log.debug("Accepting $accepting")
             try {
                 def d = whelk.get(new URI(path), version, accepting)
                 if (d && (mode== DisplayMode.META || !d.entry['deleted'])) {

                     LinkExpander le = getLinkExpanderFor(d)
                     if (le) {
                         d = le.expand(d)
                     }
                     if (mode == DisplayMode.META) {
                         sendResponse(response, d.metadataAsJson, "application/json")
                     } else {
                         def ctheader = contextHeaders.get(path.split("/")[1])
                         if (ctheader) {
                             response.setHeader("Link", "<$ctheader>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"")
                         }
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
                 def entry = [
                 "contentType":request.getContentType()
                 ]
                 Document doc = new Document().withData(request.getInputStream().getBytes()).withEntry(entry).withMeta(request.getParameterMap())
                 def identifier = convertAndSaveDocument(doc)

                 log.debug("Saved document $identifier")

                 def locationRef = request.getRequestURL()
                 while (locationRef[-1] == '/') {
                     locationRef.deleteCharAt(locationRef.length()-1)
                 }
                 locationRef.append(identifier)

                     sendDocumentSavedResponse(response, locationRef.toString(), doc.timestamp as String)

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
                     def meta = request.getParameterMap()

                     // Check If-Match
                     String ifMatch = request.getHeader("If-Match")
                     if (ifMatch
                             && this.whelk.get(new URI(path))
                             && this.whelk.get(new URI(path))?.timestamp as String != ifMatch) {
                         response.sendError(response.SC_PRECONDITION_FAILED, "The resource has been updated by someone else. Please refetch.")
                     } else {
                         try {
                             Document doc = new Document(["entry":entry,"meta":meta]).withData(request.getInputStream().getBytes())

                             identifier = convertAndSaveDocument(doc)
                             sendDocumentSavedResponse(response, request.getRequestURL().toString(), doc.timestamp as String)

                         } catch (DocumentException de) {
                             log.warn("Document exception: ${de.message}")
                             response.sendError(HttpServletResponse.SC_BAD_REQUEST, de.message)
                         } catch (WhelkAddException wae) {
                             log.warn("Whelk failed to store document: ${wae.message}")
                             response.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE , wae.message)
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

     URI convertAndSaveDocument(Document doc) {
         doc = this.whelk.sanityCheck(doc)
         log.debug("Saving document first pass.")
         def identifier = this.whelk.add(doc)
         for (fc in plugins.findAll { it instanceof FormatConverter && it.requiredContentType == doc.contentType }) {
             try {
                 log.debug("Running formatconverter ${fc.id} on ${doc.identifier}")
                     doc = fc.convert(doc)
                     doc = this.whelk.sanityCheck(doc)
                     identifier = this.whelk.add(doc)
             } catch (WhelkAddException wae) {
                 log.warn("Converted to ${doc.contentType} but there are no storages for that.")
             }
         }
     }


     void sendDocumentSavedResponse(HttpServletResponse response, String locationRef, String timestamp) {
         response.setHeader("Location", locationRef)
         response.setHeader("ETag", timestamp as String)
         response.setStatus(HttpServletResponse.SC_CREATED, "Thank you! Document ingested.")
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

     LinkExpander getLinkExpanderFor(Document doc) { return plugins.find { it instanceof LinkExpander && it.valid(doc) } }
 }

enum DisplayMode {
    DOCUMENT, META
}

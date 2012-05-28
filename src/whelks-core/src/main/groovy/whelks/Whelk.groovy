package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.TeeOutputStream

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.persistance.*

import se.kb.libris.conch.data.WhelkSearchResult

@Log
class WhelkImpl extends BasicWhelk {

    def URI generate_identifier() {
        def uri = _create_random_URI()
        while (has_identifier(uri)) {
            uri = _create_random_URI()
        }
        return uri
    }


    @Override
    void notify(URI u) {
        log.debug "Whelk $name notified of change in URI $u"
        Document doc = manager.resolve(u)
        boolean converted = false
        for (Plugin p: getPlugins()) {
            if (p instanceof FormatConverter) {
                doc = ((FormatConverter)p).convert(doc, null, null, null);
                converted = (doc != null)
            }
        }
        if (converted) {
            log.debug "Document ${doc.identifier} converted. "
            doc = rewriteIdentifierForWhelk(doc)
            log.debug "New identifier assigned."
            log.debug "Now saving new version."
            store(doc)
        }
    }

    // TODO: This seems like bad form. Need to rethink identifier concept for whelk2whelk notifications.
    private Document rewriteIdentifierForWhelk(Document doc) {
        def parts = doc.identifier.toString().split("/")
        def newUriString = new StringBuffer()
        parts.eachWithIndex() { part, i ->
            if (i == 0) {
                newUriString << (this.defaultIndex ? "/" + this.defaultIndex : "")
            }
            else if (i > 1 && part) {
                newUriString << "/" + part 
            }
        }
        doc = doc.withIdentifier(new URI(newUriString.toString()))
        log.debug "New URI: ${doc.identifier}"  
        return doc
    }

    boolean isBinaryData(byte[] data) {
        return false
    }

    boolean belongsHere(Document d) {
        return d.identifier.toString().startsWith("/"+this.name+"/")
    }

    @Override
    URI store(Document d) {
        if (! belongsHere(d)) {
            throw new WhelkRuntimeException("Document does not belong here.")
        }
        URI u = super.store(d)
        manager.notifyListeners(u)
        return u
    }

    def has_identifier(uri) {
        // TODO: implement properly
        return false
    }

    def _create_random_URI() {
        def generator = { String alphabet, int n ->
            new Random().with {
                (1..n).collect { alphabet[ nextInt( alphabet.length() ) ] }.join()
            }
        }
        return new URI("/" + this.name + "/" + generator( (('A'..'Z')+('a'..'z')+('0'..'9')).join(), 8 ))
    }

    def getApis() {
        def apis = []
        this.plugins.each {
            log.debug("getApis looping component ${it.class.name}")
            if (it instanceof API) {
                log.debug("Adding ${it.class.name} to list ...")
                apis << it
            }
        }
        return apis
    }

    @Override
    def Document get(identifier, raw=false) {
        if (identifier instanceof String) {
            identifier = new URI(identifier)
        }
        def doc = null
        plugins.each {
            if (it instanceof Storage) {
                log.debug "${it.class.name} is storage. Retrieving ..."
                doc = it.get(identifier, raw)
            }
        }
        if (doc == null) {
            throw new WhelkRuntimeException("Document not found: $identifier")
        }
        return doc
    }


    @Override
    def SearchResult query(String query, boolean raw = false) {
        def result = null
        plugins.each {
            log.debug "Looping component ${it.class.name}"
            if (it instanceof Index) {
                log.debug "Is index. Searching ..."
                result = it.query(query, raw)
                if (result != null) {
                    log.debug "Found a ${result.class.name}"
                }
            }
        }
        log.debug "Located document from elastic search"
        return result
    }

    @Override
    Document createDocument() {
        return new BasicDocument()
    }
}

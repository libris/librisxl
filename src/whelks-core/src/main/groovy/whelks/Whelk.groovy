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


@Log
class WhelkImpl extends BasicWhelk {

    WhelkImpl() {super()}
    WhelkImpl(pfx) {super(pfx)}

    @Override
    void notify(URI u) {
        log.debug "Whelk $prefix notified of change in URI $u"
        Document doc = manager.resolve(u)
        boolean converted = false
        for (Plugin p: getPlugins()) {
            if (p instanceof FormatConverter) {
                log.debug "Found a formatconverter: ${p.class.name}"
                doc = ((FormatConverter)p).convert(this, doc, null, null, null);
                converted = (doc != null)
            }
        }
        if (converted) {
            log.debug "Document ${doc.identifier} converted."
            //store(doc)
        }
    }

    boolean isBinaryData(byte[] data) {
        return false
    }

    boolean belongsHere(Document d) {
        return !d.identifier || d.identifier.toString().startsWith("/"+this.prefix+"/")
    }

    @Override
    URI store(Document d) {
        if (! belongsHere(d)) {
            throw new WhelkRuntimeException("Document does not belong here.")
        }
        try {
            return super.store(d)
        } catch (WhelkRuntimeException wre) {
            log.error("Failed to save document ${d.identifier}: " + wre.getMessage())
        }

        return null
    }

    /*
    def getApis() {
        def apis = []
        this.plugins.each {
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
    */
}

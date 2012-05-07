package se.kb.libris.conch.plugin

import groovy.util.logging.Slf4j as Log

import org.restlet.Restlet
import org.restlet.Request
import org.restlet.Response
import org.restlet.data.MediaType

import se.kb.libris.conch.Whelk
import se.kb.libris.whelks.exception.WhelkRuntimeException


interface Plugin {
    def setWhelk(Whelk w)
}
interface API extends Plugin {
    def getPath()
}





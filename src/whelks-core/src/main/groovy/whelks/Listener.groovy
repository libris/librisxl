package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*

@Log
class Listener implements Plugin {

    Whelk homewhelk
    Whelk otherwhelk
    FormatConverter converter

    String id = "whelkListener"

    Listener(Whelk n, FormatConverter conv) {
        this.otherwhelk = n
        this.converter = conv
    }

    void setWhelk(Whelk w) {
        this.homewhelk = w
        this.otherwhelk.addPluginIfNotExists(new Notifier(this))
        id = id + ", listening to $otherwhelk.prefix"
        id = id + " for $w.prefix"
    }

    void notify(Date timestamp) {
        log.debug "Whelk $homewhelk.prefix notified of change since $timestamp"
        if (converter) {
            otherwhelk.log(timestamp).each {
                Document doc = otherwhelk.get(it.identifier)
                Document convertedDocument = converter.convert(doc)
                if (convertedDocument) {
                    log.debug "New document created/converted with identifier ${convertedDocument.identifier}"
                        homewhelk.store(convertedDocument)
                }
            }
        }
    }
}

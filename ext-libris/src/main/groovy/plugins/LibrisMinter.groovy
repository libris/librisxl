package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

@Log
class LibrisMinter extends BasicPlugin implements URIMinter {

    final static long OUR_EPOCH = Date.parse("yyyy-MM-dd", "2014-01-01").getTime()

    URI mint(Document doc, boolean remint = true) {
        if (!remint && doc.identifier) {
            return new URI(doc.identifier)
        }

        return new URI("/epoch/"+getEpoch())
        // This is the last resort.
        return new URI("/"+ UUID.randomUUID())
    }

    String getEpoch() {
        int value = System.currentTimeMillis() - OUR_EPOCH
        log.info("Value is $value")
        return Integer.toString(value, 30)
    }


}

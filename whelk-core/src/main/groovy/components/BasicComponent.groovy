package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*

import org.codehaus.jackson.map.ObjectMapper

import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.*

@Log
abstract class BasicComponent extends BasicPlugin implements Component {

    public static final ObjectMapper mapper = new ObjectMapper()
    Whelk whelk
    List contentTypes

    boolean master = false

    final void bootstrap(String whelkId) {
        assert whelk

        componentBootstrap(whelkId)
    }

    abstract void componentBootstrap(String str)

    public final void start() {
        assert whelk
        log.debug("Calling onStart() on sub classes")
        onStart()
    }

    void onStart() {
        log.debug("[${this.id}] onStart() not overridden.")
    }

    @Override
    public boolean handlesContent(String ctype) {
        return (ctype == "*/*" || !this.contentTypes || this.contentTypes.contains(ctype))
    }


}

package whelk.component

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*

import org.codehaus.jackson.map.ObjectMapper

import whelk.exception.*
import whelk.plugin.*
import whelk.*

@Log
abstract class BasicComponent extends BasicPlugin implements Component {

    public static final ObjectMapper mapper = new ObjectMapper()
    Whelk whelk
    List contentTypes

    final void bootstrap() {
        assert whelk

        componentBootstrap(whelk.id)
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

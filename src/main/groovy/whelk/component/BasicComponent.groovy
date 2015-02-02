package whelk.component

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*

import whelk.exception.*
import whelk.plugin.*
import whelk.*

@Log
abstract class BasicComponent extends BasicPlugin implements Component {

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
        return (!this.contentTypes || this.contentTypes.contains("*/*") || this.contentTypes.contains(ctype))
    }


}

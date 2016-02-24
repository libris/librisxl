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
    final static String VERSION_STORAGE_SUFFIX = "_versions"

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

    protected Document createTombstone(id, dataset, entry) {
        Document tombstone = whelk.createDocument("text/plain").withIdentifier(id).withEntry(entry).withData("DELETED ENTRY")
        tombstone.setContentType("text/plain")
        tombstone.entry['deleted'] = true
        tombstone.entry['dataset'] = dataset
        return tombstone
    }

}

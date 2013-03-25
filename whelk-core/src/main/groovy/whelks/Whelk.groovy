package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log


/*
 * StandardWhelk (formerly WhelkImpl/BasicWhelk) has moved to StandardWhelk.groovy.
 * CombinedWhelk has moved to CombinedWhelk.groovy.
 * WhelkOperator has moved to WhelkOperator.groovy.
 */

@Log
class ReindexOnStartupWhelk extends StandardWhelk {

    ReindexOnStartupWhelk(String pfx) {
        super(pfx)
    }

    @Override
    public void init() {
        log.info("Indexing whelk ${this.prefix}.")
        reindex()
        log.info("Whelk reindexed.")
    }
}


@Log
class ResourceWhelk extends StandardWhelk {

    ResourceWhelk(String prefix) {
        super(prefix)
    }

}

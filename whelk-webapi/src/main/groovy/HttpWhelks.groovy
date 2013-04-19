package se.kb.libris.whelks.http

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

public interface HttpWhelk extends Whelk {
    public String getContentRoot()
}

@Log
class HttpStandardWhelk extends StandardWhelk implements HttpWhelk {
    String contentRoot

    HttpStandardWhelk(String name) {
        super(name)
        setContentRoot(name)
    }

    public void setContentRoot(String r) {
        this.contentRoot = r?.replaceAll(/\/$/, "")
        log.info("ContentRoot set to $contentRoot")
    }
}

@Log
class HttpCombinedWhelk extends CombinedWhelk implements HttpWhelk {
    String contentRoot

    HttpCombinedWhelk(String name) {
        super(name)
        setContentRoot(name)
    }

    public void setContentRoot(String r) {
        this.contentRoot = r?.replaceAll(/\/$/, "")
        log.info("ContentRoot set to $contentRoot")
    }
}

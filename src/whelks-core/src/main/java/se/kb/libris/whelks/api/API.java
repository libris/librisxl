package se.kb.libris.whelks.api;

import se.kb.libris.whelks.plugin.Plugin;

public interface API extends Plugin {
    public void request(APIRequestContext context);
}

package se.kb.libris.whelks.api;

import java.util.List;
import java.util.Map;

import se.kb.libris.whelks.plugin.WhelkAware;

public interface API extends WhelkAware {
    public ApiResult handle(Map requestMap, List pathVars, String remoteIp, String method);
}

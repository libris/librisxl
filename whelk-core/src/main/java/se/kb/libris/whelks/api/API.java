package se.kb.libris.whelks.api;

import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import se.kb.libris.whelks.plugin.WhelkAware;

public interface API extends WhelkAware {
    void handle(HttpServletRequest request, HttpServletResponse response, List pathVars);
}

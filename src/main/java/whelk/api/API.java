package whelk.api;

import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import whelk.plugin.WhelkAware;

public interface API extends WhelkAware {
    void handle(HttpServletRequest request, HttpServletResponse response, List pathVars);
}

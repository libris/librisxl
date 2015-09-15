package whelk.rest.api;

import java.util.List;
import java.util.regex.Pattern;
import javax.servlet.http.*;

public interface RestAPI {
    public void handle(HttpServletRequest request, HttpServletResponse response, List pathVars);
    public Pattern getPathPattern();
}

package whelk.rest.api;

import java.util.List;
import javax.servlet.http.*;

public interface RestAPI {
    public void handle(HttpServletRequest request, HttpServletResponse response, List pathVars);
}

package whelk.apixserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

public class ApixSearchServlet extends HttpServlet
{
    private static final Logger s_logger = LogManager.getLogger(ApixSearchServlet.class);

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        try { doGet2(request, response); } catch (Exception e)
        {
            s_logger.error("Failed to process GET request.", e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    public void doGet2(HttpServletRequest request, HttpServletResponse response) throws Exception
    {
        Set<String> searchResults = search(request);
        System.out.println(searchResults);
    }

    private Set<String> search(HttpServletRequest request)
    {
        Set<String> results = new HashSet<>();

        Enumeration<String> parameterNames = request.getParameterNames();
        while (parameterNames.hasMoreElements())
        {
            String parameterName = parameterNames.nextElement();
            String parameterValue = request.getParameter(parameterName);

            // Supported searchable identifiers are ISBN, ISSN and "024$a"

            if (parameterName.equalsIgnoreCase("isbn"))
            {
                String normalizedValue = parameterValue.replaceAll("-", "");
                List<String> systemIDs = Utils.s_whelk.getStorage().getSystemIDsByTypedID("ISBN", normalizedValue.toUpperCase(), 1);
                systemIDs.addAll( Utils.s_whelk.getStorage().getSystemIDsByTypedID("ISBN", normalizedValue.toLowerCase(), 1) );
                intersect(results, systemIDs);
            } else if (parameterName.equalsIgnoreCase("issn"))
            {
                String normalizedValue = parameterValue.replaceAll("-", "");
                List<String> systemIDs = Utils.s_whelk.getStorage().getSystemIDsByTypedID("ISSN", normalizedValue.toUpperCase(), 1);
                systemIDs.addAll( Utils.s_whelk.getStorage().getSystemIDsByTypedID("ISSN", normalizedValue.toLowerCase(), 1) );
                intersect(results, systemIDs);
            } else // if (...) what is the search term for 024$a = TODO, tighten this.
            {
                List<String> systemIDs = Utils.s_whelk.getStorage().getSystemIDsByTypedID("Identifier", parameterValue, 1);
                intersect(results, systemIDs);
            }
        }

        return results;
    }

    // Multiple parameters are interpreted as implicit ANDs (all must be satisfied).
    private void intersect(Set<String> set, List<String> list)
    {
        if (set.isEmpty())
            set.addAll(list);
        else
            set.retainAll(list);
    }
}

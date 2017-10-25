package whelk.apixserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;

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
        Set<String> resultingIDs = search(request);

        Map<String, Document> resultingDocumentsMap = Utils.s_whelk.bulkLoad(new ArrayList<>(resultingIDs));
        List<Document> resultingDocuments = new ArrayList<>();
        for (String key : resultingDocumentsMap.keySet())
            resultingDocuments.add(resultingDocumentsMap.get(key));

        Utils.send200Response(response, Xml.formatApixSearchResponse(resultingDocuments));
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
                results.addAll(systemIDs);
            } else if (parameterName.equalsIgnoreCase("issn"))
            {
                String normalizedValue = parameterValue.replaceAll("-", "");
                List<String> systemIDs = Utils.s_whelk.getStorage().getSystemIDsByTypedID("ISSN", normalizedValue.toUpperCase(), 1);
                systemIDs.addAll( Utils.s_whelk.getStorage().getSystemIDsByTypedID("ISSN", normalizedValue.toLowerCase(), 1) );
                results.addAll(systemIDs);
            } else // if (...) what is the search term for 024$a = TODO, tighten this. (urnnbn ?)
            {
                List<String> systemIDs = Utils.s_whelk.getStorage().getSystemIDsByTypedID("Identifier", parameterValue, 1);
                results.addAll(systemIDs);
            }
        }

        return results;
    }
}

package whelk.rest.api;

import groovy.lang.Tuple2;
import whelk.Document;
import whelk.Whelk;
import whelk.util.LegacyIntegrationTools;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static whelk.util.Jackson.mapper;

public class RecordRelationAPI extends WhelkHttpServlet {
    public RecordRelationAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    public RecordRelationAPI(Whelk whelk) {
        this.whelk = whelk;
        init(whelk);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String id = request.getParameter("id");
        String relation = request.getParameter("relation");
        String reverseString = request.getParameter("reverse");
        String returnMode = request.getParameter("return");


        boolean reverse = false;
        if (reverseString != null && reverseString.equals("true")) {
            reverse = true;
        }

        if (id == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "\"id\" parameter required.");
            return;
        }

        id = LegacyIntegrationTools.fixUri(id);
        String systemId = whelk.getStorage().getSystemIdByIri(id);
        if (systemId == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"id\"-parameter must refer to an existing bibliographic record.");
            return;
        }

        ArrayList<String> result = new ArrayList<>();
        if (relation == null) {
            List<Tuple2<String, String>> dependencySystemIDs;
            if (reverse) {
                dependencySystemIDs = whelk.getStorage().followDependers(systemId);
            } else {
                dependencySystemIDs = whelk.getStorage().followDependencies(systemId);
            }

            for (Tuple2<String, String> dependencySystemId : dependencySystemIDs) {
                result.add(dependencySystemId.getV1());
            }
        }
        else {
            List<String> dependencySystemIDs;
            if (reverse) {
                dependencySystemIDs = whelk.getStorage().getDependersOfType(systemId, relation);
            } else {
                dependencySystemIDs = whelk.getStorage().getDependenciesOfType(systemId, relation);
            }

            result.addAll(dependencySystemIDs);
        }

        String jsonString = null;

        if (returnMode == null || returnMode.equals("id")) {
            List<String> ids = new ArrayList<>(result.size());
            for (String resultId : result) {
                ids.add(Document.getBASE_URI().toString() + resultId);
            }
            jsonString = mapper.writeValueAsString(ids);
        }
        else if (returnMode.equals("bare_record")) {
            List<Map<String, Object>> records = new ArrayList<>(result.size());
            for (String resultId : result) {
                records.add(whelk.getStorage().load(resultId).data);
            }
            jsonString = mapper.writeValueAsString(records);
        }
        else if (returnMode.equals("embellished_record")) {
            List<Map<String, Object>> records = new ArrayList<>(result.size());
            for (String resultId : result) {
                records.add(whelk.loadEmbellished(resultId).data);
            }
            jsonString = mapper.writeValueAsString(records);
        }
        else {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"return\"-parameter must be either id (default), bare_record or embellished_record.");
            return;
        }

        response.setContentType("application/json");
        response.setHeader("Expires", "0");
        response.setHeader("Cache-Control", "no-cache");
        OutputStream out = response.getOutputStream();
        out.write(jsonString.getBytes(StandardCharsets.UTF_8));
        out.close();
    }
}

package whelk.rest.api;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.datatool.Script;
import whelk.datatool.WhelkTool;
import whelk.datatool.bulkchange.BulkChange;
import whelk.datatool.bulkchange.BulkChangeDocument;
import whelk.datatool.form.FormDiff;
import whelk.util.WhelkFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static whelk.util.Unicode.stripPrefix;

public class BulkChangePreviewAPI extends HttpServlet {

    private static final String BULK_CHANGE_PREVIEW_TYPE = "BulkChangePreview";

    private Whelk whelk;


    @Override
    public void init() {
        whelk = WhelkFactory.getSingletonWhelk();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            String id = request.getParameter("@id");
            if (id == null) {
                throw new BadRequestException("@id parameter is required");
            }
            if (!id.startsWith(Document.getBASE_URI().toString())) {
                throw new Crud.NotFoundException("Document not found");
            }
            id = stripPrefix(id, Document.getBASE_URI().toString());

            int limit = parsePositiveInt(request, "_limit", 5);
            int offset = parsePositiveInt(request, "_offset", 0);

            var doc = load(id);

            Map<Object, Object> result = new HashMap<>();
            result.put(JsonLd.TYPE_KEY, BULK_CHANGE_PREVIEW_TYPE);

            switch (doc.getSpecification()) {
                case BulkChangeDocument.FormSpecification formSpecification -> {
                    var match = FormDiff.withoutMarkerIds(formSpecification.matchForm());
                    var ids = whelk.getSparqlQueryClient().queryIdsByForm(match);
                    result.put("totalItems", ids.size());
                    result.put("items", Collections.emptyList());
                }
            }

            // TODO support turtle etc?
            HttpTools.sendResponse(response, result, (String) MimeTypes.getJSONLD());
        } catch (Exception e) {
            HttpTools.sendError(response, HttpTools.mapError(e), e.getMessage(), e);
        }
    }

    private BulkChangeDocument load(String id) {
        Document doc = whelk.getDocument(id);
        if (doc == null) {
            throw new Crud.NotFoundException("Document not found");
        }
        try {
            return new BulkChangeDocument(doc.data);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e.getMessage());
        }
    }

    static int parsePositiveInt(HttpServletRequest request, String param, int defaultValue) {
        try {
            if (request.getParameter(param) == null) {
                return defaultValue;
            }
            int i =  Integer.parseInt(request.getParameter(param));
            if (i < 0) {
                throw new NumberFormatException();
            }
            return i;
        } catch (NumberFormatException e) {
            throw new BadRequestException(String.format("%s must be a positive integer", param));
        }
    }
}

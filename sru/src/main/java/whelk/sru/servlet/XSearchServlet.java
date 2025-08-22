package whelk.sru.servlet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.exception.InvalidQueryException;
import whelk.search2.AppParams;
import whelk.search2.ESSettings;
import whelk.search2.Query;
import whelk.search2.QueryParams;
import whelk.search2.VocabMappings;
import whelk.util.DocumentUtil;
import whelk.util.FresnelUtil;
import whelk.util.http.HttpTools;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Templates;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;
import static whelk.JsonLd.asList;
import static whelk.util.DocumentUtil.getAtPath;

/**
 * Implementation of Libris legacy xsearch API on XL
 * <a href="https://libris.kb.se/help/xsearch_swe.jsp?open=tech">docs</a>
 * <a href="https://libris.kb.se/xsearch?query=qwerty&format=json&start=1">example request</a>
 * <p>
 * Tries to match the behaviour of the old API. e.g. return error as XML with code 200 even when json requested.
 * I haven't bothered looking at the old implementation yet. API behavior is just reverse engineered from API responses.
 * </p>
 */
public class XSearchServlet extends WhelkHttpServlet {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
    private final TransformerFactory transformerFactory = TransformerFactory.newInstance();

    private static final int DEFAULT_N = 10;
    private static final int MAX_N = 200;
    private static final int DEFAULT_START = 1;

    private static final Map<String, Format> FORMATS = Map.of(
            "marcxml", Format.MARC_XML,
            "json", Format.JSON,
            "mods", Format.MODS,
            "ris", Format.UNSUPPORTED,
            "dc", Format.UNSUPPORTED,
            "rdfdc", Format.UNSUPPORTED,
            "bibtex", Format.UNSUPPORTED,
            "refworks", Format.UNSUPPORTED,
            "harvard", Format.UNSUPPORTED,
            "oxford", Format.UNSUPPORTED
    );

    private static final Map<String, String> ORDER = Map.of(
            // "rank" is default
            "alphabetical", "_sortKeyByLang.sv",
            "-alphabetical", "-_sortKeyByLang.sv",
            "chronological", "-publication.year", // reverse of XL
            "-chronological", "publication.year"
    );

    private enum Format {
        MARC_XML,
        MODS,
        JSON,
        JSON_ORG,
        UNSUPPORTED,
    }

    // https://libris.kb.se/help/xsearch_swe.jsp?open=tech
    private static class Params {
        public static final String QUERY = "query";
        public static final String FORMAT = "format";
        public static final String START = "start";
        public static final String N = "n";
        public static final String ORDER = "order";
        // TODO
        public static final String FORMAT_LEVEL = "format_level";
        public static final String HOLDINGS = "holdings";
        public static final String DATABASE = "database";
    }

    // match old behaviour
    public static class Errors {
        public static final String EMPTY_QUERY = "empty_query";
        public static final String PARSE = "parse";
    }

    JsonLD2MarcXMLConverter converter;
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    VocabMappings vocabMappings;
    ESSettings esSettings;

    private record Transform(Templates templates, String contentType) { }
    Map<Format, Transform> transformers;

    @Override
    protected void init(Whelk whelk) {
        converter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());
        vocabMappings = new VocabMappings(whelk);
        esSettings = new ESSettings(whelk);

        try {
            transformers = Map.of(
                    Format.MODS, new Transform(loadXslt("transformers/MARC21slim2MODS3.xsl"), "text/xml"),
                    Format.JSON, new Transform(loadXslt("transformers/MARC21slim2JSON.xsl"), "application/json")
            );
        } catch (IOException | TransformerConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        try {
            doGet2(req, res);
        } catch (InvalidQueryException e) {
            sendErrorXml(res, e.getMessage());
        }
    }

    public void doGet2(HttpServletRequest req, HttpServletResponse res) throws IOException, InvalidQueryException {
        Map<String, String[]> parameters = req.getParameterMap();

        var query = getOptionalSingleNonEmpty(Params.QUERY, parameters)
                .orElseThrow(() -> new InvalidQueryException(Errors.EMPTY_QUERY));

        int start = getOptionalSingleNonEmpty(Params.START, parameters)
                .map(x -> parseInt(x, DEFAULT_START))
                .map(x -> Math.max(x, DEFAULT_START))
                .orElse(DEFAULT_START);

        int n = getOptionalSingleNonEmpty(Params.N, parameters)
                .map(x -> parseInt(x, DEFAULT_N))
                .map(x -> Math.min(x, MAX_N))
                .filter(x -> x >= 0) // negative -> default, same behavior as old xsearch
                .orElse(DEFAULT_N);

        var format = getOptionalSingleNonEmpty(Params.FORMAT, parameters)
                .map(f -> FORMATS.getOrDefault(f, Format.MARC_XML))
                .orElse(Format.MARC_XML);

        if (format == Format.UNSUPPORTED) {
            throw new InvalidQueryException("format unsupported"); // TODO
        }

        String sort = getOptionalSingleNonEmpty(Params.ORDER, parameters)
                .map(ORDER::get)
                .orElse(null);

        try {
            // TODO handle onr (record ID) here or in search2?

            String instanceOnlyQueryString = "(" + query + ") AND type=Instance";

            // This part is a little weird
            HashMap<String, String[]> paramsAsIfSearch = new HashMap<>();
            String[] q = new String[]{ instanceOnlyQueryString };
            paramsAsIfSearch.put("_q", q);
            paramsAsIfSearch.put("_stats", new String[]{"false"}); // don't need facets
            paramsAsIfSearch.put("_offset", new String[]{"" + (start - 1)});
            paramsAsIfSearch.put("_limit", new String[]{"" + n});
            if (sort != null) {
                paramsAsIfSearch.put("_sort", new String[]{ sort });
            }

            QueryParams qp = new QueryParams(paramsAsIfSearch);
            AppParams ap = new AppParams(new HashMap<>(), qp);
            var results = new Query(qp, ap, vocabMappings, esSettings, whelk).collectResults();

            @SuppressWarnings("unchecked")
            List<Map<?,?>> items = (List<Map<?,?>>) results.get("items");
            int totalItems = (Integer) results.get("totalItems");
            int to = Math.min(start + n, totalItems);

            switch (format) {
                case MARC_XML -> sendMarcXML(res, items, start, to, totalItems);
                case MODS -> sendTransformedMarc(res, Format.MODS, items, start, to, totalItems);
                case JSON -> sendJson(res, items, start, to, totalItems);
                // This seems to generate too much data, e.g. multiple contributors.
                // Too rich MARC as input?
                // case JSON -> sendTransformedMarc(res, Format.JSON, items, start, to, totalItems);
            }

        } catch (InvalidQueryException e) {
            logger.error("Bad query.", e);
            throw new InvalidQueryException(Errors.PARSE);
        } catch (XMLStreamException | TransformerException e) {
            logger.error("Couldn't build xsearch response.", e);
            throw new RuntimeException(e);
        }
    }

    private void sendMarcXML(HttpServletResponse res,
                             List<Map<?,?>> items,
                             int from,
                             int to,
                             int totalItems) throws IOException, XMLStreamException {
        res.setCharacterEncoding("UTF-8");
        res.setContentType("text/xml");
        writeMarcXml(res.getOutputStream(), items, from, to, totalItems);
    }

    private void writeMarcXml(OutputStream o,
                              List<Map<?,?>> items,
                              int from,
                              int to,
                              int totalItems) throws XMLStreamException {

        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(o);

        writer.writeStartDocument("UTF-8", "1.0");

        writer.writeStartElement("xsearch");
        writer.writeAttribute("xmlns:marc", "http://www.loc.gov/MARC21/slim");
        writer.writeAttribute("to", "" + to);
        writer.writeAttribute("from", "" + from);
        writer.writeAttribute("records", "" + totalItems);

        writer.writeStartElement("collection");
        writer.writeAttribute("xmlns", "http://www.loc.gov/MARC21/slim");

        // skip "category" element

        items.parallelStream()
                .map(i -> {
                    String systemID = whelk.getStorage().getSystemIdByIri( (String) i.get("@id"));
                    Document embellished = whelk.loadEmbellished(systemID);
                    return (String) converter.convert(embellished.data, embellished.getShortId()).get(JsonLd.NON_JSON_CONTENT_KEY);
                }).forEachOrdered( convertedText -> {
                    try {
                        copyRecord(xmlInputFactory.createXMLStreamReader(new StringReader(convertedText)), writer);
                    } catch (XMLStreamException e) {
                        throw new RuntimeException(e);
                    }
                });

        writer.writeEndElement(); // collection

        writer.writeEndElement(); // xsearch
        writer.writeEndDocument();

        writer.close();

    }

    private void sendErrorXml(HttpServletResponse res, String errorMsg) throws IOException {
        try {
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(res.getOutputStream());

            writer.writeStartElement("xsearch");
            writer.writeAttribute("error", errorMsg );

            writer.writeEndElement();

            writer.close();
        } catch (XMLStreamException e) {
            logger.error("Couldn't build xsearch response.", e);
        }
    }

    // copy without record tag attributes
    // old API           : <record>
    // XL marc converter : <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">
    private static void copyRecord(XMLStreamReader reader, XMLStreamWriter writer) throws XMLStreamException {
        while (reader.hasNext()) {
            switch (reader.next()) {
                case START_ELEMENT -> {
                    writer.writeStartElement(reader.getLocalName());
                    if (!reader.getLocalName().equals("record")) {
                        for (int i = 0; i < reader.getAttributeCount(); i++) {
                            String name = reader.getAttributeLocalName(i);
                            String value = reader.getAttributeValue(i);
                            writer.writeAttribute(name, value);
                        }
                    }
                }

                case END_ELEMENT -> writer.writeEndElement();
                case CHARACTERS -> writer.writeCharacters(reader.getText());
            }
        }
    }

    private void sendTransformedMarc(HttpServletResponse res,
                             Format format,
                             List<Map<?,?>> items,
                             int from,
                             int to,
                             int totalItems) throws IOException, XMLStreamException, TransformerException {

        Transform transform = transformers.get(format);
        res.setContentType(transform.contentType);
        res.setCharacterEncoding("UTF-8");

        ByteArrayOutputStream o = new ByteArrayOutputStream();
        writeMarcXml(o, items, from, to, totalItems);
        ByteArrayInputStream i = new ByteArrayInputStream(o.toByteArray());

        transform.templates.newTransformer().transform(new StreamSource(i), new StreamResult(res.getOutputStream()));
    }

    private Templates loadXslt(String name) throws IOException, TransformerConfigurationException {
        var url = Thread.currentThread().getContextClassLoader().getResource(name);
        assert url != null;
        var xsltSource = new StreamSource(url.openStream(), url.toExternalForm());
        return transformerFactory.newTemplates(xsltSource);
    }

    // Or convert from MARC-XML?
    // https://git.kb.se/libris/legacy/search/-/blob/master/src/main/webapp/transformers/MARC21slim2JSON.xsl?ref_type=heads
    private void sendJson(HttpServletResponse res,
                          List<Map<?,?>> items,
                          int from,
                          int to,
                          int totalItems) {
        var result = Map.of("xsearch", Map.of(
                "from", from,
                "to", to,
                "records", totalItems,
                "items", items.stream().map(this::toXsearchJson).toList()
        ));

        HttpTools.sendResponse(res, result, "application/json;charset=UTF-8");
    }

    private Map<?, ?> toXsearchJson(Map<?, ?> item) {
        Function<Object, String> format = (Object o) -> whelk.getFresnelUtil().format(
                whelk.getFresnelUtil().applyLens(o, FresnelUtil.LensGroupName.Chip),
                new FresnelUtil.LangCode("sv")
        ).asString();


        var result = new LinkedHashMap<String, Object>();
        result.put("identifier", "http://libris.kb.se/bib/" + getAtPath(item, List.of("meta", "controlNumber")));

        /*
        Always one

        "title": "Röda rummet : skildringar ur artist- och författarlifvet",

         */
        var titles = get(item, List.of("hasTitle"));
        titles.stream()
                .filter(t -> "Title".equals(t.get(JsonLd.TYPE_KEY)))
                .findFirst()
                .or(() -> titles.stream().findFirst())
                .map(format)
                .ifPresent(t -> result.put("title", t));


        /*
        Always one

        creator": "Strindberg, August, 1849-1912",

         */
        var contribution = get(item, List.of("instanceOf", "contribution"));
        contribution.stream()
                .filter(t -> "PrimaryContribution".equals(t.get(JsonLd.TYPE_KEY)))
                .findFirst()
                .or(() -> contribution.stream()
                        .filter(c -> get(c, List.of("role", "*"))
                                .stream()
                                .anyMatch(r -> "https://id.kb.se/relator/author".equals(r.get(JsonLd.ID_KEY))))
                        .findFirst())
                .or(() -> contribution.stream().findFirst())
                .filter(c -> c.containsKey("agent"))
                .map(c -> c.get("agent"))
                .map(format)
                .ifPresent(t -> result.put("creator", t));

        /*

        "isbn": "9519519025",

        "isbn": [
          "9781637846193",
          "9781637846186 (invalid)"
        ],

        "issn": "1403-3844",

         */
        // TODO XL search result includes generated ISBN10 or ISBN13
        var isbns = get(item, List.of("identifiedBy")).stream()
                .filter(t -> "ISBN".equals(t.get(JsonLd.TYPE_KEY)))
                .filter(c -> c.containsKey("value"))
                .map(c -> c.get("value"))
                .toList();
        if (!isbns.isEmpty()) {
            result.put("isbn", unwrapSingle(isbns));
        }

        var issns = get(item, List.of("identifiedBy")).stream()
                .filter(t -> "ISSN".equals(t.get(JsonLd.TYPE_KEY)))
                .filter(c -> c.containsKey("value"))
                .map(c -> c.get("value"))
                .toList();
        if (!issns.isEmpty()) {
            result.put("issn", unwrapSingle(issns));
        }

        /*
        TODO map new types/categories from type normalization
        "type": "book",
        "type": "journal",
        "type": "E-book",
         */
        result.put("type", "TODO...");


        /*
        One string publication + One string manufacture

        "publisher": "Enskede : TPB",
        "date": "2007",


        "publisher": [
          "Stockholm : Norstedt",
          "Norge"
        ],
        "date": "1988",


        "publisher": [
          "[Stockholm] : The Sublunar Society",
          "Stockholm : F4-print AB"
        ],
        "date": [
          "[2021]",
          "2021"
        ],

         */
        var allPublications = get(item, List.of("publication"));
        var publications = new ArrayList<Map<?,?>>(allPublications.size());
        publications.addAll(allPublications.stream().filter(p -> "PrimaryPublication".equals(p.get(JsonLd.TYPE_KEY))).toList());
        publications.addAll(allPublications.stream().filter(p -> !"PrimaryPublication".equals(p.get(JsonLd.TYPE_KEY))).toList());
        var dates = new TreeSet<>();
        var dateFields = List.of("year", "date", "startYear", "endYear");

        var manufacture = get(item, List.of("manufacture"));

        DocumentUtil.Visitor filter = (value,path) -> {
            if (!path.isEmpty()) {
                if (dateFields.contains(path.getLast())) {
                    dates.add(value.toString());
                    return new DocumentUtil.Remove();
                }
                if ("country".equals(path.getLast())) {
                    return new DocumentUtil.Remove();
                }
            }

            return DocumentUtil.NOP;
        };
        DocumentUtil.traverse(publications, filter);
        DocumentUtil.traverse(manufacture, filter);
        var publisher = new ArrayList<String>();
        if (!publications.isEmpty()) {
            publisher.add(publications.stream()
                    .map(format)
                    .collect(Collectors.joining(", "))); // old xsearch uses a :
        }
        if (!manufacture.isEmpty()) {
            publisher.add(manufacture.stream()
                    .map(format)
                    .collect(Collectors.joining(", "))); // old xsearch uses a :
        }
        if (!publisher.isEmpty()) {
            result.put("publisher", unwrapSingle(publisher));
        }
        if (!dates.isEmpty()) {
            result.put("date", unwrapSingle(dates));
        }


        /*

        "language": "swe",

         */
        var langs = get(item, List.of("instanceOf", "language", "*", "code"));
        if (!langs.isEmpty()) {
            result.put("language", unwrapSingle(langs));
        }


        /*
        This is a bit weird. It is mixed URI:s and notes/labels

        "free": [
          "http://www.rodarummet.org"
        ]


        "free": [
          "http://urn.kb.se/resolve?urn=urn:nbn:se:alvin:portal:record-433139",
          [
            "Fritt tillgänglig via Uppsala universitetsbibliotek"
          ]
        ]

         */
        var free = new ArrayList<>();
        Stream.concat(
                        get(item, List.of("associatedMedia", "*")).stream(),
                        get(item, List.of("@reverse", "reproductionOf", "*", "associatedMedia", "*")).stream())
                .filter(t -> "MediaObject".equals(t.get(JsonLd.TYPE_KEY)))
                .forEach(m -> {
                    if (m.containsKey("uri")) {
                        free.addAll(asList(m.get("uri")));
                    }
                    if (m.containsKey("marc:publicNote")) {
                        free.add(asList(m.get("marc:publicNote")));
                    }
                });

        if (!free.isEmpty()) {
            result.put("free", free); // always array
        }

        /*
        TODO ?
        @reverse, itemOf, associatedMedia ?
        (not in search result, need to load embellished record)

        "urls": {
          "JonE": [
            [
              "https://primo.library.ju.se/openurl/JUL/46JUL_INST:jul?u.ignore_date_coverage=true&rft.mms_id=997999292403831",
              [
                "Online access for JON",
                "fulltext",
                "Project Runeberg"
              ]
            ]
          ],
          "LnuE": [
            [
              "http://link.lnu.se/openurl?u.ignore_date_coverage=true&rft.mms_id=997038027003661",
              [
                "Online access for Linnaeus University",
                "fulltext",
                "Project Runeberg"
              ]
            ]
          ],

         */

        //result.put("DEBUG ORG", item);

        // TODO any other fields?

        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<?,?>> get(Object o, List<String> path) {
       return ((List<Map<?,?>>) getAtPath(o, path, Collections.emptyList()));
    }

    private static Optional<String> getOptionalSingleNonEmpty(String name, Map<String, String[]> queryParameters) {
        return getOptionalSingle(name, queryParameters).filter(Predicate.not(String::isEmpty));
    }

    private static Optional<String> getOptionalSingle(String name, Map<String, String[]> queryParameters) {
        return Optional.ofNullable(queryParameters.get(name))
                .map(x -> x[0]);
    }

    private static int parseInt(String s, Integer defaultTo) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ignored) {
            return defaultTo;
        }
    }

    private static Object unwrapSingle(Object value) {
        if (value instanceof List<?> list && list.size() == 1) {
            return list.getFirst();
        }
        if (value instanceof Set<?> set && set.size() == 1) {
            return set.stream().toList().getFirst();
        }
        return value;
    }
}

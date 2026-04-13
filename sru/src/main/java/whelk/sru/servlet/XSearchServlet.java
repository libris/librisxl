package whelk.sru.servlet;

import groovy.lang.Tuple2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.util.marc.Field;
import se.kb.libris.util.marc.MarcFieldComparator;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import se.kb.libris.utils.isbn.ConvertException;
import se.kb.libris.utils.isbn.Isbn;
import se.kb.libris.utils.isbn.IsbnException;
import se.kb.libris.utils.isbn.IsbnParser;
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
import javax.xml.transform.Transformer;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
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
import static se.kb.libris.export.ExportProfile.addSabTitles;
import static se.kb.libris.export.ExportProfile.mergeBibMfhd;
import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.NONE_KEY;
import static whelk.JsonLd.Platform.CATEGORY_BY_COLLECTION;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.WORK_KEY;
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
    Map<Format, Templates> transformers;

    @Override
    protected void init(Whelk whelk) {
        converter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());
        vocabMappings = VocabMappings.load(whelk);
        esSettings = new ESSettings(whelk);

        try {
            transformers = Map.of(
                    Format.MODS, loadXslt("transformers/MARC21slim2MODS3.xsl")
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

        var includeHoldings = getOptionalSingleNonEmpty(Params.HOLDINGS, parameters)
                .map("true"::equals).orElse(false)
                && (format == Format.MARC_XML || format == Format.MODS);

        var include9xx = getOptionalSingleNonEmpty(Params.FORMAT_LEVEL, parameters)
                .map("full"::equals).orElse(false);

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
            AppParams ap = new AppParams(new HashMap<>(), whelk.getJsonld());
            var results = new Query(qp, ap, vocabMappings, esSettings, whelk).collectResults();

            @SuppressWarnings("unchecked")
            List<Map<?,?>> items = (List<Map<?,?>>) results.get("items");
            int totalItems = (Integer) results.get("totalItems");
            int to = Math.min(start + n, totalItems);

            switch (format) {
                case MARC_XML -> sendMarcXML(res, items, start, to, totalItems, includeHoldings, include9xx);
                case JSON -> sendJson(res, items, start, to, totalItems);
                case MODS -> sendTransformedMarc(res, Format.MODS, items, start, to, totalItems, includeHoldings, include9xx);
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
                             int totalItems,
                             boolean includeHoldings,
                             boolean include9xx) throws IOException, XMLStreamException {
        res.setCharacterEncoding("UTF-8");
        res.setContentType("text/xml");
        OutputStream out = res.getOutputStream();
        writeMarcXml(out, items, from, to, totalItems, includeHoldings, include9xx);
        out.flush();
        out.close();
    }

    private void writeMarcXml(OutputStream o,
                              List<Map<?,?>> items,
                              int from,
                              int to,
                              int totalItems,
                              boolean includeHoldings,
                              boolean include9xx) throws XMLStreamException {

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
                    var bibXml = (String) converter.convert(embellished.data, embellished.getShortId())
                            .get(JsonLd.NON_JSON_CONTENT_KEY);

                    bibXml = expandRecord(bibXml, embellished, includeHoldings, include9xx);

                    return bibXml;
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
            OutputStream out = res.getOutputStream();
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(out);

            writer.writeStartElement("xsearch");
            writer.writeAttribute("error", errorMsg );

            writer.writeEndElement();

            writer.close();
            out.flush();
            out.close();
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
                             int totalItems,
                             boolean includeHoldings,
                             boolean include9xx) throws IOException, XMLStreamException, TransformerException {

        res.setCharacterEncoding("UTF-8");
        res.setContentType("text/xml");

        ByteArrayOutputStream o = new ByteArrayOutputStream();
        writeMarcXml(o, items, from, to, totalItems, includeHoldings, include9xx);
        ByteArrayInputStream i = new ByteArrayInputStream(o.toByteArray());

        Transformer transformer = transformers.get(format).newTransformer();

        OutputStream out = res.getOutputStream();
        transformer.transform(new StreamSource(i), new StreamResult(res.getOutputStream()));
        out.flush();
        out.close();
    }

    private Templates loadXslt(String name) throws IOException, TransformerConfigurationException {
        var url = Thread.currentThread().getContextClassLoader().getResource(name);
        assert url != null;
        var xsltSource = new StreamSource(url.openStream(), url.toExternalForm());
        return transformerFactory.newTemplates(xsltSource);
    }

    private String expandRecord(String bibXml, Document bib, boolean includeHoldings, boolean include9xx) {
        if (!includeHoldings && !include9xx) {
            return bibXml;
        }

        try {
            MarcRecord bibRecord = MarcXmlRecordReader.fromXml(bibXml);

            ListIterator<Field> li = bibRecord.listIterator();
            while (li.hasNext()) {
                if (Objects.equals((li.next()).getTag(), "003")) {
                    li.remove();
                }
            }
            bibRecord.addField(bibRecord.createControlfield("003", "SE-LIBR"), MarcFieldComparator.strictSorted);

            if (includeHoldings) {
                List<Document> holdingDocuments = whelk.getAttachedHoldings(bib.getThingIdentifiers());
                for (Document holding : holdingDocuments) {
                    var holdXml = (String) converter.convert(holding.data, holding.getShortId())
                            .get(JsonLd.NON_JSON_CONTENT_KEY);
                    MarcRecord holdRecord = MarcXmlRecordReader.fromXml(holdXml);
                    mergeBibMfhd(bibRecord, holding.getHeldBySigel(), holdRecord);
                }
            }

            if (include9xx) {
                // Only 976...
                addSabTitles(bibRecord);
            }

            ByteArrayOutputStream o = new ByteArrayOutputStream();
            var w = new MarcXmlRecordWriter(o, false);
            w.writeRecord(bibRecord);
            w.close();
            return o.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Or convert from MARC-XML?
    // https://git.kb.se/libris/legacy/search/-/blob/master/src/main/webapp/transformers/MARC21slim2JSON.xsl?ref_type=heads
    private void sendJson(HttpServletResponse res,
                          List<Map<?,?>> items,
                          int from,
                          int to,
                          int totalItems) {

        var result = new LinkedHashMap<String, Object>();
        result.put("from", from);
        result.put("to", to);
        result.put("records", totalItems);
        result.put("list", items.stream().map(this::toXsearchJson).toList());

        var response = Map.of("xsearch", result);

        HttpTools.sendResponse(res, response, "application/json;charset=UTF-8");
    }

    private Map<?, ?> toXsearchJson(Map<?, ?> item) {
        Function<Object, String> format = (Object o) -> whelk.getFresnelUtil().asFormattedString(o,
                FresnelUtil.NestedLenses.CHIP_TO_TOKEN,
                "sv");

        var result = new LinkedHashMap<String, Object>();
        result.put("identifier", "http://libris.kb.se/bib/" + getAtPath(item, List.of("meta", "controlNumber")));

        /*
        Always one

        "title": "Röda rummet : skildringar ur artist- och författarlifvet",

         */
        var titles = get(item, List.of("hasTitle"));
        titles.stream()
                .filter(t -> "Title".equals(t.get(TYPE_KEY)))
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
                .filter(t -> "PrimaryContribution".equals(t.get(TYPE_KEY)))
                .findFirst()
                .or(() -> contribution.stream()
                        .filter(c -> get(c, List.of("role", "*"))
                                .stream()
                                .anyMatch(r -> "https://id.kb.se/relator/author".equals(r.get(ID_KEY))))
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

        var isbns = new ArrayList<>(get(item, List.of("identifiedBy")).stream()
                .filter(t -> "ISBN".equals(t.get(TYPE_KEY)))
                .filter(c -> c.containsKey("value"))
                .map(c -> new Tuple2<>((String) c.get("value"), (Object) c.get("qualifier")))
                .toList());

        // XL search result includes generated ISBN10 or ISBN13.
        // Remove ISBN10 without qualifier if corresponding ISBN13 exists
        isbns.removeIf(i -> {
            var i13 = toIsbn13(i.getV1());
            var hasNoQualifier = i.getV2() == null;
            return i13 != null && hasNoQualifier && isbns.stream().anyMatch(i2 -> i13.equals(i2.getV1()));
        });

        if (!isbns.isEmpty()) {
            result.put("isbn", unwrapSingle(isbns.stream().map(Tuple2::getV1).toList()));
        }

        var issns = get(item, List.of("identifiedBy")).stream()
                .filter(t -> "ISSN".equals(t.get(TYPE_KEY)))
                .filter(c -> c.containsKey("value"))
                .map(c -> c.get("value"))
                .toList();
        if (!issns.isEmpty()) {
            result.put("issn", unwrapSingle(issns));
        }

        /*
        "type": "book",
        "type": "journal",
        "type": "E-book",
         */
        result.put("type", getLegacyWebbsokType(item));

        /*
        One string per publication + One string per manufacture

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
        publications.addAll(allPublications.stream().filter(p -> "PrimaryPublication".equals(p.get(TYPE_KEY))).toList());
        publications.addAll(allPublications.stream().filter(p -> !"PrimaryPublication".equals(p.get(TYPE_KEY))).toList());
        var dates = new ArrayList<>();
        var dateFields = List.of("year", "date");

        var manufacture = get(item, List.of("manufacture"));

        DocumentUtil.Visitor filter = (value,path) -> {
            if (!path.isEmpty()) {
                if (dateFields.contains(path.getLast())) {
                    if (value instanceof List<?> l) {
                        dates.add(String.join(", ", l.stream().map(String::valueOf).toList()));
                    } else {
                        dates.add(value.toString());
                    }
                    return new DocumentUtil.Remove();
                }
                if ("country".equals(path.getLast())) {
                    return new DocumentUtil.Remove();
                }
                if (value instanceof Map<?,?> m && (m.containsKey("startYear") || m.containsKey("endYear"))) {
                    var start = m.remove("startYear");
                    var end = m.remove("endYear");
                    dates.add(String.format("%s-%s", start != null ? start : "", end != null ? end : ""));
                }
            }

            return DocumentUtil.NOP;
        };
        DocumentUtil.traverse(publications, filter);
        DocumentUtil.traverse(manufacture, filter);
        var publisher = new ArrayList<String>();
        publications.stream()
                    .map(format)
                    .forEach(publisher::add);
        manufacture.stream()
                .map(format)
                .forEach(publisher::add);
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
                .filter(t -> "MediaObject".equals(t.get(TYPE_KEY)))
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

    private static String toIsbn13(String isbn) {
        if (isbn == null || isbn.length() != 10) {
            return null;
        }
        try {
            var other = IsbnParser.parse(isbn);
            if (other == null) {
                return null;
            }
            return other.convert(Isbn.ISBN13).toString();
        } catch (IsbnException | ConvertException ignored) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<?,?>> get(Object o, List<String> path) {
       return ((List<Map<?,?>>) getAtPath(o, path, Collections.emptyList()));
    }

    @SuppressWarnings("unchecked")
    private static List<String> getS(Object o, List<String> path) {
        return ((List<String>) getAtPath(o, path, Collections.emptyList()));
    }

    private static String getUriSlug(String s) {
        return List.of(s.split("/")).getLast();
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

    private static String getLegacyWebbsokType(Map<?, ?> item) {
        var workType = (String) ((Map<?, ?>) item.get(WORK_KEY)).get(TYPE_KEY);
        var instanceType = (String) asList(item.get(TYPE_KEY)).getFirst();

        var workCategories = new HashSet<String>();
        getS(item, List.of(WORK_KEY, CATEGORY_BY_COLLECTION, "find", "*", ID_KEY))
                .stream().map(XSearchServlet::getUriSlug)
                .forEach(workCategories::add);
        getS(item, List.of(WORK_KEY, CATEGORY_BY_COLLECTION, "identify", "*", ID_KEY))
                .stream().map(XSearchServlet::getUriSlug)
                .forEach(workCategories::add);
        getS(item, List.of(WORK_KEY, CATEGORY_BY_COLLECTION, NONE_KEY, "*", ID_KEY))
                .stream().map(XSearchServlet::getUriSlug)
                .forEach(workCategories::add);

        var instanceCategories = getS(item, List.of(CATEGORY_BY_COLLECTION, NONE_KEY, "*", ID_KEY))
                .stream().map(XSearchServlet::getUriSlug)
                .collect(Collectors.toSet());

        return getLegacyWebbsokType(instanceType, workType, instanceCategories, workCategories);
    }

    private static String getLegacyWebbsokType(
            String instanceType,
            String workType,
            Set<String> instanceCategories,
            Set<String> workCategories) {

        for (var r : TYPE_RULES) {
            if (r.match(instanceType, workType, instanceCategories, workCategories)) {
                return r.result;
            }
        }

        return "";
    }

    private record Rule(String result, String instanceType, String workType, Set<String> instanceCategories, Set<String> workCategories) {
        boolean match(String instanceType, String workType, Set<String> instanceCategories, Set<String> workCategories) {
            return ((this.instanceType == null || this.instanceType.equals(instanceType))
                    && (this.workType == null || this.workType.equals(workType))
                    && (instanceCategories.containsAll(this.instanceCategories))
                    && (workCategories.containsAll(this.workCategories))
            );
        }
    }

    // https://git.kb.se/libris/legacy/search/-/blob/master/src/main/webapp/transformers/MARC21slim2JSON.xsl?ref_type=heads#L207
    /*
        <xsl:when test="$leader6-7 = 'am' and $cf007 = 'cr'">E-book</xsl:when>
        <xsl:when test="$leader6-7 = 'am' and $cf007 != 'cr'">book</xsl:when>
        <xsl:when test="($leader6-7 = 'aa' or $leader6-7 = 'ab') and $cf007 = 'cr'">E-article</xsl:when>
        <xsl:when test="($leader6-7 = 'aa' or $leader6-7 = 'ab') and $cf007 != 'cr'">article</xsl:when>
        <xsl:when test="$leader6-7 = 'as' and $cf007 = 'cr'">E-journal</xsl:when>
        <xsl:when test="$leader6-7 = 'as' and $cf007 != 'cr'">journal</xsl:when>
        <xsl:when test="$leader6='t'">manuscript</xsl:when>
        <xsl:when test="$leader6='a'">text</xsl:when>
        <xsl:when test="$leader6='e' or $leader6='f'">cartographic</xsl:when>
        <xsl:when test="$leader6='c' or $leader6='d'">notated music</xsl:when>
        <xsl:when test="$leader6='i'">sound recording</xsl:when>
        <xsl:when test="$leader6='j'">musical sound recording</xsl:when>
        <xsl:when test="$leader6='k'">still image</xsl:when>
        <xsl:when test="$leader6='g'">moving image</xsl:when>
        <xsl:when test="$leader6='r'">three dimensional object</xsl:when>
        <xsl:when test="$leader6='m'">software, multimedia</xsl:when>
        <xsl:when test="$leader6='p'">mixed material</xsl:when>
        <xsl:when test="$leader6='o'">kit</xsl:when>
     */

    // evaluated in this order. first matching applies.
    private static final Rule[] TYPE_RULES = {
            new Rule(
                    "kit",
                    "PhysicalResource",
                    "Monograph",
                    Set.of(),
                    Set.of("Kit")
            ),
            new Rule(
                    "book",
                    null,
                    "Monograph",
                    Set.of("Print", "Volume"),
                    Set.of()
            ),
            new Rule(
                    "E-book",
                    null,
                    null,
                    Set.of("EBook"),
                    Set.of()
            ),
            new Rule(
                    "E-article",
                    "DigitalResource",
                    null,
                    Set.of("ComponentPart"),
                    Set.of()
            ),
            new Rule(
                    "article",
                    null,
                    null,
                    Set.of("ComponentPart"),
                    Set.of()
            ),
            new Rule(
                    "E-journal",
                    "DigitalResource",
                    "Serial",
                    Set.of(),
                    Set.of()
            ),
            new Rule(
                    "journal",
                    null,
                    "Serial",
                    Set.of(),
                    Set.of()
            ),
            new Rule(
                    "manuscript",
                    null,
                    null,
                    Set.of(),
                    Set.of("Handskrifter")
            ),
            new Rule(
                    "software, multimedia",
                    null,
                    null,
                    Set.of(),
                    Set.of("Software")
            ),
            new Rule(
                    "moving image",
                    null,
                    null,
                    Set.of(),
                    Set.of("MovingImage")
            ),
            new Rule(
                    "still image",
                    null,
                    null,
                    Set.of(),
                    Set.of("Bilder")
            ),
            new Rule(
                    "notated music",
                    null,
                    null,
                    Set.of(),
                    Set.of("Noterad%20musik")
            ),
            new Rule(
                    "musical sound recording",
                    null,
                    null,
                    Set.of(),
                    Set.of("Musik")
            ),
            new Rule(
                    "sound recording",
                    null,
                    null,
                    Set.of(),
                    Set.of("Tal")
            ),
            new Rule(
                    "sound recording",
                    null,
                    null,
                    Set.of(),
                    Set.of("Audio")
            ),
            new Rule(
                    "sound recording",
                    null,
                    null,
                    Set.of(),
                    Set.of("Ljudb%C3%B6cker")
            ),
            new Rule(
                    "cartographic",
                    null,
                    null,
                    Set.of(),
                    Set.of("Kartografiskt%20material")
            ),
            new Rule(
                    "three dimensional object",
                    null,
                    null,
                    Set.of(),
                    Set.of("ThreeDimensionalForm")
            ),
            new Rule(
                    "text",
                    null,
                    null,
                    Set.of(),
                    Set.of("Text")
            )
    };
}

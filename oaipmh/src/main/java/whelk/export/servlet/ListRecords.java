package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

import io.prometheus.client.Counter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ListRecords
{
    private final static String FROM_PARAM = "from";
    private final static String UNTIL_PARAM = "until";
    private final static String SET_PARAM = "set";
    private final static String RESUMPTION_PARAM = "resumptionToken";
    private final static String FORMAT_PARAM = "metadataPrefix";
    private final static String DELETED_DATA_PARAM = "x-withDeletedData";
    private final static String INCLUDE_SILENT_PARAM = "x-withSilentUpdates";

    // Number of source rows from lddb to scan per page. One page = one HTTP response,
    // possibly with a resumptionToken for the next page.
    private final static int PAGE_SIZE = 1000;

    private static final Counter failedRequests = Counter.build()
            .name("oaipmh_failed_listrecords_requests_total").help("Total failed ListRecords requests.")
            .labelNames("error").register();

    private final static Logger logger = LogManager.getLogger(ListRecords.class);


    /**
     * Verifies the integrity of a OAI-PMH request with the verb 'ListRecords', sends a proper response.
     * @param onlyIdentifiers When this is set to true, the response will be formatted as a ListIdentifiers response.
     *                        When it is false, the response will be formatted as a ListRecords response.
     */
    public static void handleListRecordsRequest(HttpServletRequest request, HttpServletResponse response,
                                                boolean onlyIdentifiers)
            throws IOException, XMLStreamException, SQLException
    {
        String resumptionTokenParam = request.getParameter(RESUMPTION_PARAM);
        String from, until, set, metadataPrefix;
        boolean withDeletedData, withSilentChanges;
        Timestamp afterModified = null;
        String afterId = null;

        if (resumptionTokenParam != null)
        {
            if (ResponseCommon.errorOnExtraParameters(request, response, RESUMPTION_PARAM))
                return;

            ResumptionToken token = ResumptionToken.parse(resumptionTokenParam);
            if (token == null)
            {
                failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_RESUMPTION_TOKEN).inc();
                ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_RESUMPTION_TOKEN,
                        "The resumptionToken is invalid or has expired.", request, response);
                return;
            }

            from = token.from;
            until = token.until;
            set = token.set;
            metadataPrefix = token.metadataPrefix;
            withDeletedData = token.withDeletedData;
            withSilentChanges = token.withSilentChanges;
            if (token.afterModified != null)
            {
                afterModified = Timestamp.from(ZonedDateTime.parse(token.afterModified).toInstant());
                afterId = token.afterId;
            }
        }
        else
        {
            from = request.getParameter(FROM_PARAM); // optional
            until = request.getParameter(UNTIL_PARAM); // optional
            set = request.getParameter(SET_PARAM); // optional
            metadataPrefix = request.getParameter(FORMAT_PARAM); // required

            // optional and not technically legal OAI-PMH
            withDeletedData = Boolean.parseBoolean(request.getParameter(DELETED_DATA_PARAM));
            withSilentChanges = Boolean.parseBoolean(request.getParameter(INCLUDE_SILENT_PARAM));

            if (ResponseCommon.errorOnExtraParameters(request, response,
                    FROM_PARAM, UNTIL_PARAM, SET_PARAM, FORMAT_PARAM, DELETED_DATA_PARAM, INCLUDE_SILENT_PARAM))
                return;

            if (metadataPrefix == null)
            {
                failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT).inc();
                ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                        "metadataPrefix argument required.", request, response);
                return;
            }

            // If client doesn't supply an "until" parameter, make one and put it in the resumptionToken,
            // so we can guarantee that the harvest terminates.
            if (until == null)
                until = ZonedDateTime.now(ZoneOffset.UTC).toString();
        }

        // Was the set selection valid?
        SetSpec setSpec = new SetSpec(set);
        if (!setSpec.isValid())
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "Not a supported set spec: " + set, request, response);
            return;
        }

        // Was the data ordered in a format we know?
        if (!OaiPmh.supportedFormats.containsKey(metadataPrefix))
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT,
                    "Unsupported format: " + metadataPrefix,
                    request, response);
            return;
        }

        ZonedDateTime fromDateTime;
        ZonedDateTime untilDateTime;
        try
        {
            fromDateTime = Helpers.parseISO8601(from);
            untilDateTime = Helpers.parseISO8601(until);
        } catch (DateTimeParseException dtpe)
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "Allowed time formats are: YYYY-MM-DD and YYYY-MM-DDThh:mm:ssZ.", request, response);
            return;
        }

        try (Connection dbconn = OaiPmh.s_whelk.getStorage().getOuterConnection())
        {
            dbconn.setAutoCommit(false);
            boolean includeDependencies = metadataPrefix.contains(OaiPmh.FORMAT_EXPANDED_POSTFIX) ||
                    metadataPrefix.contains("marcxml");

            ResumptionToken baseToken = new ResumptionToken(from, until, set, metadataPrefix,
                    withDeletedData, withSilentChanges, null, null);

            try (Helpers.ResultIterator resultIterator = Helpers.getMatchingDocuments(dbconn, fromDateTime,
                    untilDateTime, setSpec, null, includeDependencies, withSilentChanges,
                    afterModified, afterId, PAGE_SIZE))
            {
                respond(request, response, metadataPrefix, onlyIdentifiers,
                        includeDependencies, withDeletedData, resultIterator, baseToken,
                        resumptionTokenParam != null);
            } finally {
                dbconn.commit();
            }
        }
    }

    private static void respond(HttpServletRequest request, HttpServletResponse response,
                                String requestedFormat, boolean onlyIdentifiers, boolean embellish,
                                boolean withDeletedData, Helpers.ResultIterator resultIterator,
                                ResumptionToken baseToken, boolean resuming)
            throws IOException, XMLStreamException, SQLException
    {
        if (!resultIterator.hasNext() && !resuming && resultIterator.isExhausted())
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH, "", request, response);
            return;
        }

        // Build the xml response feed
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

        ResponseCommon.writeOaiPmhHeader(writer, request, true);

        if (onlyIdentifiers)
            writer.writeStartElement("ListIdentifiers");
        else
            writer.writeStartElement("ListRecords");

        while (resultIterator.hasNext())
        {
            ResponseCommon.emitRecord(resultIterator.next(), writer, requestedFormat,
                    onlyIdentifiers, embellish, withDeletedData);
        }

        writeResumptionToken(writer, resultIterator, baseToken, resuming);

        writer.writeEndElement(); // ListIdentifiers/ListRecords
        ResponseCommon.writeOaiPmhClose(writer, request);
    }

    private static void writeResumptionToken(XMLStreamWriter writer, Helpers.ResultIterator resultIterator,
                                             ResumptionToken baseToken, boolean resuming)
            throws XMLStreamException
    {
        // If we've previously emitted a non-empty resumptionToken and there are no more records,
        // then per the spec we have to end with an empty resumptionToken.
        if (resultIterator.isExhausted())
        {
            if (resuming)
            {
                writer.writeStartElement("resumptionToken");
                writer.writeEndElement();
            }
            return;
        }

        ResumptionToken next = baseToken.withCursor(
                resultIterator.getLastSourceModified(), resultIterator.getLastSourceId());
        writer.writeStartElement("resumptionToken");
        writer.writeCharacters(next.toToken());
        writer.writeEndElement();
    }
}

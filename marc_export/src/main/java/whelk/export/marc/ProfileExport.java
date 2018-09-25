package whelk.export.marc;
import groovy.lang.Tuple2;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import whelk.Document;
import whelk.Whelk;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import se.kb.libris.export.ExportProfile;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.util.LegacyIntegrationTools;
import whelk.util.MarcExport;

public class ProfileExport
{
    private JsonLD2MarcXMLConverter m_toMarcXmlConverter;
    public ProfileExport(Whelk whelk)
    {
        m_toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.createMarcFrameConverter());
    }

    /**
     * Export MARC data from 'whelk' affected in between 'from' and 'until' shaped by 'profile' into 'output'.
     */
    public OutputStream exportInto(OutputStream output, ExportProfile profile, Whelk whelk, String from, String until)
            throws IOException, SQLException
    {
        ZonedDateTime zonedFrom = ZonedDateTime.parse(from);
        ZonedDateTime zonedUntil = ZonedDateTime.parse(until);
        Timestamp fromTimeStamp = new Timestamp(zonedFrom.toInstant().getEpochSecond() * 1000L);
        Timestamp untilTimeStamp = new Timestamp(zonedUntil.toInstant().getEpochSecond() * 1000L);

        try(Connection connection = whelk.getStorage().getConnection();
            PreparedStatement preparedStatement = getAllChangedIDsStatement(fromTimeStamp, untilTimeStamp, connection);
            ResultSet resultSet = preparedStatement.executeQuery())
        {
            while (resultSet.next())
            {
                String id = resultSet.getString("id");
                String collection = resultSet.getString("collection");
                String changedBy = resultSet.getString("changedBy");
                Timestamp createdTime = resultSet.getTimestamp("created");

                boolean created = true;
                if (zonedFrom.toInstant().isBefore(createdTime.toInstant()) &&
                        zonedUntil.toInstant().isAfter(createdTime.toInstant()))
                    created = false;

                exportAffectedDocuments(id, collection, changedBy, created, profile, output, whelk);
            }
        }

        return null;
    }

    /**
     * Export (into output) all documents that are affected by 'id' having been updated by 'changedBy'.
     * 'created' == true means 'id' was created in the chosen interval, false means merely updated.
     */
    private void exportAffectedDocuments(String id, String collection, String changedBy, boolean created,
                                                ExportProfile profile, OutputStream output, Whelk whelk)
            throws IOException
    {
        TreeSet<String> exportedIDs = new TreeSet<>();

        if (collection.equals("bib"))
        {
            if (profile.getProperty("bibcreate", "ON").equalsIgnoreCase("OFF") && created)
                return; // Created records not requested
            if (profile.getProperty("bibupdate", "ON").equalsIgnoreCase("OFF"))
                return; // Updated records not requested
            Set<String> biboperators = profile.getSet("biboperators");
            if ( !biboperators.isEmpty() && changedBy != null && !biboperators.contains( changedBy ))
                return; // Updates from this operator/changedBy not requested

            exportDocument(whelk.getStorage().load(id), profile, output, exportedIDs, whelk);
        }
        else if (collection.equals("auth"))
        {
            if (profile.getProperty("authcreate", "ON").equalsIgnoreCase("OFF") && created)
                return; // Created records not requested
            if (profile.getProperty("authupdate", "ON").equalsIgnoreCase("OFF"))
                return; // Updated records not requested
            Set<String> authoperators = profile.getSet("authoperators");
            if ( !authoperators.isEmpty() && changedBy != null && !authoperators.contains( changedBy ))
                return; // Updates from this operator/changedBy not requested

            List<Tuple2<String, String>> dependers = whelk.getStorage().getDependers(id);
            for (Tuple2 depender : dependers)
            {
                String dependerId = (String) depender.getFirst();
                Document dependerDoc = whelk.getStorage().load(dependerId);
                String dependerCollection = LegacyIntegrationTools.determineLegacyCollection(dependerDoc, whelk.getJsonld());
                if (dependerCollection.equals("bib"))
                    exportDocument(dependerDoc, profile, output, exportedIDs, whelk);
            }
        }
        else if (collection.equals("hold"))
        {
            if (profile.getProperty("holdcreate", "ON").equalsIgnoreCase("OFF") && created)
                return; // Created records not requested
            if (profile.getProperty("holdupdate", "ON").equalsIgnoreCase("OFF"))
                return; // Updated records not requested
            Set<String> holdoperators = profile.getSet("holdoperators");
            if ( !holdoperators.isEmpty() && changedBy != null && !holdoperators.contains( changedBy ))
                return; // Updates from this operator/changedBy not requested

            Document changedHoldDocument = whelk.getStorage().load(id);
            String itemOf = changedHoldDocument.getHoldingFor();
            exportDocument(whelk.getStorage().getDocumentByIri(itemOf), profile, output, exportedIDs, whelk);
            // ALSO CHECK VERSIONS, FIND THE PREVIOUS ITEMOF AND EXPORT THAT!
        }
    }

    /**
     * Export document (into output)
     */
    private void exportDocument(Document document, ExportProfile profile, OutputStream output,
                                       TreeSet<String> exportedIDs, Whelk whelk)
            throws IOException
    {
        String systemId = document.getShortId();
        if (exportedIDs.contains(systemId))
            return;
        exportedIDs.add(systemId);

        String encoding = profile.getProperty("characterencoding");
        if (encoding.equals("Latin1Strip")) {
            encoding = "ISO-8859-1";
        }

        Vector<MarcRecord> result = MarcExport.compileVirtualMarcRecord(profile, document, whelk, m_toMarcXmlConverter);
        if (result == null) // A conversion error will already have been logged. Anything else, and we want to fail fast.
            return;
        if (profile.getProperty("format", "ISO2709").equalsIgnoreCase("MARCXML"))
        {
            MarcXmlRecordWriter writer = new MarcXmlRecordWriter(output, encoding);
            for (MarcRecord mr : result)
                writer.writeRecord(mr);
        }
        else
        {
            Iso2709MarcRecordWriter writer = new Iso2709MarcRecordWriter(output, encoding);
            for (MarcRecord mr : result)
                writer.writeRecord(mr);
        }
    }

    /**
     * Get all records that changed in the interval
     */
    private PreparedStatement getAllChangedIDsStatement(Timestamp from, Timestamp until, Connection connection)
            throws SQLException
    {
        String sql = "SELECT id, collection, changedBy, created FROM lddb WHERE modified >= ? AND modified <= ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setTimestamp(1, from);
        preparedStatement.setTimestamp(2, until);
        return preparedStatement;
    }
}

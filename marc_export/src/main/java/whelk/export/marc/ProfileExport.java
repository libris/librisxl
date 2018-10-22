package whelk.export.marc;
import groovy.lang.Tuple2;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.MarcRecordWriter;
import whelk.Document;
import whelk.Whelk;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.time.ZonedDateTime;
import java.util.*;

import se.kb.libris.export.ExportProfile;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.util.LegacyIntegrationTools;
import whelk.util.MarcExport;

public class ProfileExport
{
    public enum DELETE_MODE
    {
        IGNORE, // Do not export deleted records
        EXPORT, // Do export deleted record
        SEND_EMAIL // Do not export deleted records, but send their IDs in an email to be manually deleted
    }

    private static final Summary totalExportCount = Summary.build().name("marc_export_total_document_count")
        .help("Number of documents in export response").register();
    private static final Summary affectedCount = Summary.build().name("marc_export_affected_document_count")
        .help("Number of affected documents in single document export").register();
    private static final Summary singleExportLatency = Summary.build().name("marc_export_single_doc_latency_seconds")
        .help("The time in seconds it takes to export a single 'complete' document")
        .labelNames("collection").register();
    private JsonLD2MarcXMLConverter m_toMarcXmlConverter;
    private Whelk m_whelk;
    public ProfileExport(Whelk whelk)
    {
        m_whelk = whelk;
        m_toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.createMarcFrameConverter());
    }

    /**
     * Export MARC data from 'whelk' affected in between 'from' and 'until' shaped by 'profile' into 'output'.
     */
    public void exportInto(MarcRecordWriter output, ExportProfile profile, String from, String until,
                                   DELETE_MODE deleteMode, boolean doVirtualDeletions)
            throws IOException, SQLException
    {
        if (profile.getProperty("status", "ON").equalsIgnoreCase("OFF"))
            return;

        ZonedDateTime zonedFrom = ZonedDateTime.parse(from);
        ZonedDateTime zonedUntil = ZonedDateTime.parse(until);
        Timestamp fromTimeStamp = new Timestamp(zonedFrom.toInstant().getEpochSecond() * 1000L);
        Timestamp untilTimeStamp = new Timestamp(zonedUntil.toInstant().getEpochSecond() * 1000L);

        TreeSet<String> exportedIDs = new TreeSet<>();
        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement preparedStatement = getAllChangedIDsStatement(fromTimeStamp, untilTimeStamp, connection);
            ResultSet resultSet = preparedStatement.executeQuery())
        {
            while (resultSet.next())
            {
                String id = resultSet.getString("id");
                String collection = resultSet.getString("collection");
                Timestamp createdTime = resultSet.getTimestamp("created");
                Boolean deleted = resultSet.getBoolean("deleted");

                boolean created = false;
                if (zonedFrom.toInstant().isBefore(createdTime.toInstant()) &&
                        zonedUntil.toInstant().isAfter(createdTime.toInstant()))
                    created = true;

                int affected = exportAffectedDocuments(id, collection, created, deleted, fromTimeStamp,
                        untilTimeStamp, profile, output, deleteMode, doVirtualDeletions, exportedIDs);
                affectedCount.observe(affected);
            }
        }
        finally {
            totalExportCount.observe(exportedIDs.size());
        }
    }

    /**
     * Export (into output) all documents that are affected by 'id' having been updated.
     * 'created' == true means 'id' was created in the chosen interval, false means merely updated.
     */
    private int exportAffectedDocuments(String id, String collection, boolean created, Boolean deleted,Timestamp from,
                                         Timestamp until, ExportProfile profile, MarcRecordWriter output,
                                         DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                         TreeSet<String> exportedIDs)
            throws IOException, SQLException
    {
        Summary.Timer requestTimer = singleExportLatency.labels(collection).startTimer();
        try
        {
            return exportAffectedDocuments2(id, collection, created, deleted, from, until, profile,
                    output, deleteMode, doVirtualDeletions, exportedIDs);
        }
        finally
        {
            requestTimer.observeDuration();
        }
    }

    private int exportAffectedDocuments2(String id, String collection, boolean created, Boolean deleted,Timestamp from,
                                         Timestamp until, ExportProfile profile, MarcRecordWriter output,
                                         DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                         TreeSet<String> exportedIDs)
            throws IOException, SQLException
    {
        int oldCount = exportedIDs.size();

        if (collection.equals("bib") && updateShouldBeExported(id, collection, profile, from, until, created, deleted))
        {
            exportDocument(m_whelk.getStorage().loadEmbellished(id, m_whelk.getJsonld()), profile,
                    output, exportedIDs, deleteMode, doVirtualDeletions);
        }
        else if (collection.equals("auth") && updateShouldBeExported(id, collection, profile, from, until, created, deleted))
        {
            List<Tuple2<String, String>> dependers = m_whelk.getStorage().getDependers(id);
            for (Tuple2 depender : dependers)
            {
                String dependerId = (String) depender.getFirst();
                Document dependerDoc = m_whelk.getStorage().loadEmbellished(dependerId, m_whelk.getJsonld());
                String dependerCollection = LegacyIntegrationTools.determineLegacyCollection(dependerDoc, m_whelk.getJsonld());
                if (dependerCollection.equals("bib"))
                    exportDocument(dependerDoc, profile, output, exportedIDs, deleteMode, doVirtualDeletions);
            }
        }
        else if (collection.equals("hold") && updateShouldBeExported(id, collection, profile, from, until, created, deleted))
        {
            List<Document> versions = m_whelk.getStorage().loadAllVersions(id);

            // Export the new/current itemOf
            Document version;
            if (versions.size() > 0)
            {
                version = versions.get(0);
            }
            else
            {
                version = m_whelk.getStorage().load(id);
            }
            String itemOf = version.getHoldingFor();
            String itemOfSystemId = m_whelk.getStorage().getSystemIdByIri(itemOf);
            if (itemOfSystemId != null) // itemOfSystemId _can_ be null, if the bib linked record is deleted (no thing-uri left in the id table)
                exportDocument(
                        m_whelk.getStorage().loadEmbellished(itemOfSystemId, m_whelk.getJsonld())
                        , profile, output, exportedIDs, deleteMode, doVirtualDeletions);

            // If the itemOf link was changed, also export the bib that is no longer linked.
            if (versions.size() > 1)
            {
                Document oldVersion = versions.get(1);
                String oldItemOf = oldVersion.getHoldingFor();
                if (!oldItemOf.equals(itemOf))
                {
                    String oldItemOfSystemId = m_whelk.getStorage().getSystemIdByIri(oldItemOf);
                    if (oldItemOfSystemId != null) // oldItemOfSystemId _can_ be null, if the bib linked record is deleted (no thing-uri left in the id table)
                        exportDocument(
                                m_whelk.getStorage().loadEmbellished(oldItemOfSystemId, m_whelk.getJsonld())
                                , profile, output, exportedIDs, deleteMode, doVirtualDeletions);
                }
            }
        }
        return exportedIDs.size() - oldCount;
    }

    /**
     * Should an update of 'id' affect the collection of bibs being exported? 'id' may be any collection!
     */
    private boolean updateShouldBeExported(String id, String collection, ExportProfile profile, Timestamp from,
                                           Timestamp until, boolean created, Boolean deleted)
            throws SQLException
    {
        if (profile.getProperty(collection+"create", "ON").equalsIgnoreCase("OFF") && created)
            return false; // Created records not requested
        if (profile.getProperty(collection+"update", "ON").equalsIgnoreCase("OFF") && !created)
            return false; // Updated records not requested
        if (profile.getProperty(collection+"delete", "ON").equalsIgnoreCase("OFF") && deleted)
            return false; // Deleted records not requested
        Set<String> operators = profile.getSet(collection+"operators");
        if ( !operators.isEmpty() )
        {
            Set<String> operatorsInInterval = getAllChangedBy(id, from, until);
            if ( !operatorsInInterval.isEmpty() ) // Ignore setting if there are no changedBy names
            {
                operatorsInInterval.retainAll(operators);
                if (operatorsInInterval.isEmpty()) // The intersection between chosen-operators and operators that changed the record is []
                    return false; // Updates from this operator/changedBy not requested
            }
        }
        
        String locations = profile.getProperty("locations", "");
        HashSet locationSet = new HashSet(Arrays.asList(locations.split(" ")));
        if ( ! locationSet.contains("*") )
        {
            Document updatedDocument = m_whelk.getStorage().load(id);

            if (collection.equals("bib"))
            {
                boolean bibIsHeld = false;
                List<Document> holdings = m_whelk.getStorage().getAttachedHoldings(updatedDocument.getThingIdentifiers(), m_whelk.getJsonld());
                for (Document holding : holdings)
                {
                    if (locationSet.contains(holding.getSigel()))
                        bibIsHeld = true;
                }
                if (!bibIsHeld)
                    return false;
            }
            if (collection.equals("hold"))
            {
                if (!locationSet.contains(updatedDocument.getSigel()))
                    return false;
            }
        }

        return true;
    }

    /**
     * Export bib document (into output)
     */
    private void exportDocument(Document document, ExportProfile profile, MarcRecordWriter output,
                                       TreeSet<String> exportedIDs, DELETE_MODE deleteMode, boolean doVirtualDeletions)
            throws IOException
    {
        String systemId = document.getShortId();
        if (exportedIDs.contains(systemId))
            return;
        exportedIDs.add(systemId);

        String locations = profile.getProperty("locations", "");
        HashSet locationSet = new HashSet(Arrays.asList(locations.split(" ")));
        if (doVirtualDeletions && !locationSet.contains("*") && deleteMode != DELETE_MODE.IGNORE)
        {
            boolean bibIsHeld = false;
            List<Document> holdings = m_whelk.getStorage().getAttachedHoldings(document.getThingIdentifiers(), m_whelk.getJsonld());
            for (Document holding : holdings)
            {
                if (locationSet.contains(holding.getSigel()))
                    bibIsHeld = true;
            }
            if (!bibIsHeld)
                document.setDeleted(true);
        }

        if (document.getDeleted())
        {
            switch (deleteMode)
            {
                case IGNORE:
                    return;
                case SEND_EMAIL:
                    throw new RuntimeException("EMAIL/MANUAL-DELETE NOT YET IMPLEMENTED");
                case EXPORT:
                    break;
            }
        }

        Vector<MarcRecord> result = MarcExport.compileVirtualMarcRecord(profile, document, m_whelk, m_toMarcXmlConverter);
        if (result == null) // A conversion error will already have been logged. Anything else, and we want to fail fast.
            return;

        for (MarcRecord mr : result)
            output.writeRecord(mr);
    }

    /**
     * Get all records that changed in the interval
     */
    private PreparedStatement getAllChangedIDsStatement(Timestamp from, Timestamp until, Connection connection)
            throws SQLException
    {
        String sql = "SELECT id, collection, created, deleted FROM lddb WHERE modified >= ? AND modified <= ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setTimestamp(1, from);
        preparedStatement.setTimestamp(2, until);
        return preparedStatement;
    }

    /**
     * Get all changedBy names for the given id and interval
     */
    private Set<String> getAllChangedBy(String id, Timestamp from, Timestamp until)
            throws SQLException
    {
        HashSet<String> result = new HashSet<>();
        Connection connection = m_whelk.getStorage().getConnection();
        connection.setAutoCommit(false);
        try(PreparedStatement preparedStatement = getAllChangedByStatement(id, from, until, connection);
            ResultSet resultSet = preparedStatement.executeQuery())
        {
            while (resultSet.next())
            {
                Timestamp modified = resultSet.getTimestamp("modified");
                String changedBy = resultSet.getString("changedBy");

                if (from.toInstant().isBefore(modified.toInstant()) &&
                        until.toInstant().isAfter(modified.toInstant()))
                    result.add(changedBy);
            }
        } finally
        {
            connection.close();
        }
        return result;
    }

    private PreparedStatement getAllChangedByStatement(String id, Timestamp from, Timestamp until, Connection connection)
            throws SQLException
    {
        String sql = "SELECT modified, changedBy FROM lddb__versions WHERE modified >= ? AND modified <= ? AND id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setFetchSize(100);
        preparedStatement.setTimestamp(1, from);
        preparedStatement.setTimestamp(2, until);
        preparedStatement.setString(3, id);
        return preparedStatement;
    }
}

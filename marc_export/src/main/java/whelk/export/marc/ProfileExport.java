package whelk.export.marc;

import groovy.lang.Tuple2;
import io.prometheus.client.Summary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.export.ExportProfile;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.MarcRecordWriter;
import whelk.Document;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.util.LegacyIntegrationTools;
import whelk.util.MarcExport;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;

public class ProfileExport
{
    private final Logger logger = LogManager.getLogger(this.getClass());

    public enum DELETE_REASON
    {
        DELETED,
        VIRTUALLY_DELETED
    }

    public enum DELETE_MODE
    {
        IGNORE, // Do not export deleted records
        EXPORT, // Do export deleted record
        SEPARATE // Do not export deleted records, but return a list of them separately
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
        m_toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());
    }

    /**
     * Export MARC data affected in between 'from' and 'until' shaped by 'profile' into 'output'.
     * Return a set of IDs that should be deleted separately and the reason why. If deleteMode is not 'SEPARATE', the
     * returned collection will be empty.
     */
    public TreeMap<String, DELETE_REASON> exportInto(MarcRecordWriter output, ExportProfile profile, String from,
                                                     String until, DELETE_MODE deleteMode, boolean doVirtualDeletions)
            throws IOException, SQLException
    {
        TreeMap<String, DELETE_REASON> deletedNotifications = new TreeMap<>();

        if (profile.getProperty("status", "ON").equalsIgnoreCase("OFF"))
            return deletedNotifications;

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
                        untilTimeStamp, profile, output, deleteMode, doVirtualDeletions, exportedIDs,
                        deletedNotifications);
                affectedCount.observe(affected);
            }
        }
        finally {
            totalExportCount.observe(exportedIDs.size());
        }

        return deletedNotifications;
    }

    /**
     * Export (into output) all documents that are affected by 'id' having been updated.
     * 'created' == true means 'id' was created in the chosen interval, false means merely updated.
     */
    private int exportAffectedDocuments(String id, String collection, boolean created, Boolean deleted,Timestamp from,
                                         Timestamp until, ExportProfile profile, MarcRecordWriter output,
                                         DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                         TreeSet<String> exportedIDs, TreeMap<String, DELETE_REASON> deletedNotifications)
            throws IOException, SQLException
    {
        Summary.Timer requestTimer = singleExportLatency.labels(collection).startTimer();
        try
        {
            return exportAffectedDocuments2(id, collection, created, deleted, from, until, profile,
                    output, deleteMode, doVirtualDeletions, exportedIDs, deletedNotifications);
        }
        finally
        {
            requestTimer.observeDuration();
        }
    }

    private int exportAffectedDocuments2(String id, String collection, boolean created, Boolean deleted,Timestamp from,
                                         Timestamp until, ExportProfile profile, MarcRecordWriter output,
                                         DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                         TreeSet<String> exportedIDs, TreeMap<String, DELETE_REASON> deletedNotifications)
            throws IOException, SQLException
    {
        int oldCount = exportedIDs.size();

        if (collection.equals("bib") && updateShouldBeExported(id, collection, profile, from, until, created, deleted))
        {
            exportDocument(m_whelk.getStorage().loadEmbellished(id, m_whelk.getJsonld()), profile,
                    output, exportedIDs, deleteMode, doVirtualDeletions, deletedNotifications);
        }
        else if (collection.equals("auth") && updateShouldBeExported(id, collection, profile, from, until, created, deleted))
        {
            List<Tuple2<String, String>> dependers = m_whelk.getStorage().getDependers(id);
            for (Tuple2 depender : dependers)
            {
                String dependerId = (String) depender.getFirst();
                Document dependerDoc = m_whelk.getStorage().loadEmbellished(dependerId, m_whelk.getJsonld());
                String dependerCollection = LegacyIntegrationTools.determineLegacyCollection(dependerDoc, m_whelk.getJsonld());
                if (!dependerCollection.equals("bib"))
                    continue;

                if (!isHeld(dependerDoc, profile))
                    continue;

                exportDocument(dependerDoc, profile, output, exportedIDs, deleteMode, doVirtualDeletions, deletedNotifications);
            }
        }
        else if (collection.equals("hold") && updateShouldBeExported(id, collection, profile, from, until, created, deleted))
        {
            List<Document> versions = m_whelk.getStorage().loadAllVersions(id);

            // The 'versions' list is sorted, with the most recent version first.
            // 1. We iterate through the list, skipping (continue) until we've found a
            // version inside the update interval.
            // 2. We keep iterating over versions and check if we're still inside the
            // interval _after_ each iteration, which means we will pass (export) all
            // versions inside the interval and exactly one version "before" the interval.
            for (Document version : versions)
            {
                String itemOf = version.getHoldingFor();
                Instant modified = ZonedDateTime.parse(version.getModified()).toInstant();

                if (modified.isAfter(until.toInstant()))
                    continue;

                String itemOfSystemId = m_whelk.getStorage().getSystemIdByIri(itemOf);
                // itemOfSystemId _can_ be null, if the bib linked record is deleted (no thing-uri left in the id table)
                if (itemOfSystemId != null) {
                    exportDocument(
                            m_whelk.getStorage().loadEmbellished(itemOfSystemId, m_whelk.getJsonld())
                            , profile, output, exportedIDs, deleteMode, doVirtualDeletions, deletedNotifications);
                } else {
                    logger.info("Not exporting {} ({}) for {} because of missing itemOf systemID", id,
                            collection, profile.getProperty("name", "unknown"));
                }

                boolean insideInterval = from.toInstant().isBefore(modified) && until.toInstant().isAfter(modified);
                if ( !insideInterval )
                    break;
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
        String profileName = profile.getProperty("name", "unknown");

        if (profile.getProperty(collection+"create", "ON").equalsIgnoreCase("OFF") && created) {
            logger.info("Not exporting created {} ({}) for {}", id, collection, profileName);
            return false; // Created records not requested
        }
        if (profile.getProperty(collection+"update", "ON").equalsIgnoreCase("OFF") && !created) {
            logger.info("Not exporting updated {} ({}) for {}", id, collection, profileName);
            return false; // Updated records not requested
        }
        if (profile.getProperty(collection+"delete", "ON").equalsIgnoreCase("OFF") && deleted) {
            logger.info("Not exporting deleted {} ({}) for {}", id, collection, profileName);
            return false; // Deleted records not requested
        }
        Set<String> operators = profile.getSet(collection+"operators");
        if ( !operators.isEmpty() )
        {
            Set<String> operatorsInInterval = getAllChangedBy(id, from, until);
            if ( !operatorsInInterval.isEmpty() ) // Ignore setting if there are no changedBy names
            {
                operatorsInInterval.retainAll(operators);
                // The intersection between chosen-operators and operators that changed the record is []
                if (operatorsInInterval.isEmpty()) {
                    logger.info("Not exporting {} ({}) for {} because of operator settings", id, collection,
                            profileName);
                    return false; // Updates from this operator/changedBy not requested
                }
            }
        }

        String locations = profile.getProperty("locations", "");
        HashSet locationSet = new HashSet(Arrays.asList(locations.split(" ")));
        if ( ! locationSet.contains("*") )
        {
            Document updatedDocument = m_whelk.getStorage().load(id);

            if (collection.equals("bib"))
            {
                if (!isHeld(updatedDocument, profile)) {
                    return false;
                }
            }
            if (collection.equals("hold"))
            {
                if (!locationSet.contains(updatedDocument.getSigel())) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Export bib document (into output)
     */
    private void exportDocument(Document document, ExportProfile profile, MarcRecordWriter output,
                                TreeSet<String> exportedIDs, DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                TreeMap<String, DELETE_REASON> deletedNotifications)
            throws IOException
    {
        String profileName = profile.getProperty("name", "unknown");
        String systemId = document.getShortId();
        if (exportedIDs.contains(systemId))
            return;
        exportedIDs.add(systemId);

        // TODO (later): Filtering: not just efilter! biblevel (encodingLevel=5) and licensefilter too!
        if (profile.getProperty("efilter", "OFF").equalsIgnoreCase("ON"))
        {
            boolean onlineResource = false;
            List<Map> carrierTypes = document.getCarrierTypes();
            if (carrierTypes != null)
            {
                for (Map map : carrierTypes)
                {
                    if ( map.containsKey("@id") && map.get("@id").equals("https://id.kb.se/marc/OnlineResource") )
                        onlineResource = true;
                }
            }
            if (document.getThingType().equals("Electronic") && onlineResource) {
                logger.info("Not exporting {} for {} because of efilter setting", systemId, profileName);
                return;
            }
        }

        String locations = profile.getProperty("locations", "");
        HashSet locationSet = new HashSet(Arrays.asList(locations.split(" ")));
        DELETE_REASON deleteReason = DELETE_REASON.DELETED; // Default
        if (doVirtualDeletions && !locationSet.contains("*"))
        {
            if (!isHeld(document, profile)) {
                if (deleteMode == DELETE_MODE.IGNORE)
                    logger.info("Not exporting {} for {} because of combined virtual deletions and ignore-deleted setting", systemId, profileName);
                document.setDeleted(true);
                deleteReason = DELETE_REASON.VIRTUALLY_DELETED;
            }
        }

        if (document.getDeleted())
        {
            switch (deleteMode)
            {
                case IGNORE:
                    return;
                case SEPARATE:
                    deletedNotifications.put(systemId, deleteReason);
                    return;
                case EXPORT:
                    break;
            }
        }

        Vector<MarcRecord> result = MarcExport.compileVirtualMarcRecord(profile, document, m_whelk, m_toMarcXmlConverter);
        // A conversion error will already have been logged. Anything else, and we want to fail fast.
        if (result == null) {
            logger.info("Not exporting {} for {} because of conversion error", systemId, profileName);
            return;
        }

        for (MarcRecord mr : result)
            output.writeRecord(mr);
    }

    boolean isHeld(Document doc, ExportProfile profile)
    {
        String locations = profile.getProperty("locations", "");
        HashSet locationSet = new HashSet(Arrays.asList(locations.split(" ")));

	if ( locationSet.contains("*") ) return true; // KP 190401
	
        List<Document> holdings = m_whelk.getStorage().getAttachedHoldings(doc.getThingIdentifiers(), m_whelk.getJsonld());
        for (Document holding : holdings)
        {
            if (locationSet.contains(holding.getSigel()))
                return true;
        }
        return false;
    }

    /**
     * Get all records that changed in the interval
     */
    private PreparedStatement getAllChangedIDsStatement(Timestamp from, Timestamp until, Connection connection)
            throws SQLException
    {
        String sql = "SELECT id, collection, created, deleted FROM lddb__versions WHERE modified >= ? AND modified <= ?";
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

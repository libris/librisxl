package whelk.export.marc;

import groovy.lang.Tuple2;
import io.prometheus.client.Summary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.export.ExportProfile;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.MarcRecordWriter;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.util.LegacyIntegrationTools;
import whelk.util.MarcExport;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.stream.Collectors;

public class ProfileExport
{
    private final Logger logger = LogManager.getLogger(this.getClass());
    private HashSet<String> workDerivativeTypes = null;

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

        // _only_ the derivatives, not "Work" itself, as that is what the "classical" MARC works use, which we
        // do not want to filter out as bib.
        workDerivativeTypes = new HashSet<>(m_whelk.getJsonld().getSubClasses("Work"));
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
        try(Connection connection = m_whelk.getStorage().getOuterConnection();
            PreparedStatement preparedStatement = getAllChangedIDsStatement(fromTimeStamp, untilTimeStamp, connection);
            ResultSet resultSet = preparedStatement.executeQuery())
        {
            while (resultSet.next())
            {
                String id = resultSet.getString("id");
                String collection = resultSet.getString("collection");
                String mainEntityType = resultSet.getString("mainEntityType");
                Timestamp createdTime = resultSet.getTimestamp("created");
                Boolean deleted = resultSet.getBoolean("deleted");

                boolean created = false;
                if (zonedFrom.toInstant().isBefore(createdTime.toInstant()) &&
                        zonedUntil.toInstant().isAfter(createdTime.toInstant()))
                    created = true;

                int affected = exportAffectedDocuments(id, collection, created, deleted, fromTimeStamp,
                        untilTimeStamp, profile, output, deleteMode, doVirtualDeletions, exportedIDs,
                        deletedNotifications, mainEntityType, connection);
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
                                        TreeSet<String> exportedIDs, TreeMap<String, DELETE_REASON> deletedNotifications,
                                        String mainEntityType, Connection connection)
            throws IOException, SQLException
    {
        Summary.Timer requestTimer = singleExportLatency.labels(collection).startTimer();
        try
        {
            return exportAffectedDocuments2(id, collection, created, deleted, from, until, profile,
                    output, deleteMode, doVirtualDeletions, exportedIDs, deletedNotifications, mainEntityType, connection);
        }
        finally
        {
            requestTimer.observeDuration();
        }
    }

    private int exportAffectedDocuments2(String id, String collection, boolean created, Boolean deleted,Timestamp from,
                                         Timestamp until, ExportProfile profile, MarcRecordWriter output,
                                         DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                         TreeSet<String> exportedIDs, TreeMap<String, DELETE_REASON> deletedNotifications,
                                         String mainEntityType, Connection connection)
            throws IOException, SQLException
    {
        int oldCount = exportedIDs.size();

        if (collection.equals("bib") && updateShouldBeExported(id, collection, mainEntityType, profile, from, until, created, deleted, connection))
        {
            exportDocument(m_whelk.loadEmbellished(id), profile,
                    output, exportedIDs, deleteMode, doVirtualDeletions, deletedNotifications);
        }
        else if (collection.equals("auth") && updateShouldBeExported(id, collection, mainEntityType, profile, from, until, created, deleted, connection))
        {
            for (String bibId : getAffectedBibIdsForAuth(id, profile))
            {
                exportDocument(m_whelk.loadEmbellished(bibId), profile, output, exportedIDs, deleteMode, doVirtualDeletions, deletedNotifications);
            }
        }
        else if (collection.equals("hold") && updateShouldBeExported(id, collection, mainEntityType, profile, from, until, created, deleted, connection))
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
                            m_whelk.loadEmbellished(itemOfSystemId)
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
    private boolean updateShouldBeExported(String id, String collection, String mainEntityType, ExportProfile profile, Timestamp from,
                                           Timestamp until, boolean created, Boolean deleted, Connection connection)
            throws SQLException
    {
        String profileName = profile.getProperty("name", "unknown");

        /*
        From a MARC perspective stuff that's now placed in the "work" section used to be a part of
        the MARC "bib" dataset.

        In order for the authupdate/bibupdate (on/off) etc settings to match what MARC-people expect,
        changes in work-records need to be evaluated using the bib settings, even though work-records
        within XL are really classified as "auth".

        In other words, a change in a work, should result in an export, _even though_ authupdate=off.

        hence:
         */
        String usingCollectionRules = collection;
        if (collection.equals("auth") && workDerivativeTypes.contains(mainEntityType))
        {
            usingCollectionRules = "bib";
        }

        if (profile.getProperty(usingCollectionRules+"create", "ON").equalsIgnoreCase("OFF") && created) {
            logger.debug("Not exporting created {} ({}) for {}", id, collection, profileName);
            return false; // Created records not requested
        }
        if (profile.getProperty(usingCollectionRules+"update", "ON").equalsIgnoreCase("OFF") && !created) {
            logger.debug("Not exporting updated {} ({}) for {}", id, collection, profileName);
            return false; // Updated records not requested
        }
        if (profile.getProperty(usingCollectionRules+"delete", "ON").equalsIgnoreCase("OFF") && deleted) {
            logger.debug("Not exporting deleted {} ({}) for {}", id, collection, profileName);
            return false; // Deleted records not requested
        }
        Set<String> operators = profile.getSet(usingCollectionRules+"operators");
        if ( !operators.isEmpty() )
        {
            Set<String> operatorsInInterval = getAllChangedBy(id, from, until, connection);
            if ( !operatorsInInterval.isEmpty() ) // Ignore setting if there are no changedBy names
            {
                operatorsInInterval.retainAll(operators);
                // The intersection between chosen-operators and operators that changed the record is []
                if (operatorsInInterval.isEmpty()) {
                    logger.debug("Not exporting {} ({}) for {} because of operator settings", id, usingCollectionRules,
                            profileName);
                    return false; // Updates from this operator/changedBy not requested
                }
            }
        }

        if ( ! profile.shouldExportAllLocations() )
        {
            if (collection.equals("bib"))
            {
                if (!isHeld(id, profile)) {
                    return false;
                }
            }
            if (collection.equals("hold"))
            {
                Document updatedDocument = m_whelk.getStorage().load(id);
                if (!profile.locations().contains(updatedDocument.getHeldBySigel())) {
                    return false;
                }
            }
        }

        return true;
    }

    private Collection<String> getAffectedBibIdsForAuth(String authId, ExportProfile profile) {
        List<String> allIds = m_whelk.getStorage()
                .followDependers(authId, JsonLd.getNON_DEPENDANT_RELATIONS())
                .stream().map(Tuple2::getFirst).collect(Collectors.toList());

        if (profile.shouldExportAllLocations()) {
            return allIds.stream()
                    .filter(id -> "bib".equals(m_whelk.getStorage().getCollectionBySystemID(id)))
                    .collect(Collectors.toList());
        }
        else {
            return m_whelk.getStorage().filterBibIdsByHeldBy(allIds, locationLibraryUris(profile));
        }
    }

    /**
     * Export bib document (into output)
     */
    private void exportDocument(Document document, ExportProfile profile, MarcRecordWriter output,
                                TreeSet<String> exportedIDs, DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                TreeMap<String, DELETE_REASON> deletedNotifications)
            throws IOException
    {
        String collection = LegacyIntegrationTools.determineLegacyCollection(document, m_whelk.getJsonld());
        if (!collection.equals("bib"))
        {
            logger.error("CRITICALLY BROKEN DATA: Was asked (but skipping) to include non bib-record in export stream: " + document.getId());
            return;
        }

        String profileName = profile.getProperty("name", "unknown");
        String systemId = document.getShortId();
        if (exportedIDs.contains(systemId))
            return;
        exportedIDs.add(systemId);

        DELETE_REASON deleteReason = DELETE_REASON.DELETED; // Default
        if (doVirtualDeletions && !profile.shouldExportAllLocations())
        {
            if (!isHeld(systemId, profile)) {
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

        for (MarcRecord mr : result) {
            String filterName = profile.findFilter(mr);
            if (filterName != null) {
                logger.debug("Not exporting {} for {} because of {} setting", systemId, profileName, filterName);
                continue;
            }
            try {
                output.writeRecord(mr);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                logger.error(String.format("Error writing %s for %s:  %s", systemId, profileName, e), e);
            }
        }
    }

    boolean isHeld(String systemId, ExportProfile profile)
    {
        if(profile.shouldExportAllLocations()) {
            return true;
        }

        return m_whelk.getStorage()
                .filterBibIdsByHeldBy(Collections.singleton(systemId), locationLibraryUris(profile))
                .contains(systemId);
    }

    private List<String> locationLibraryUris(ExportProfile profile) {
        return profile.locations().stream()
                .map(LegacyIntegrationTools::legacySigelToUri)
                .collect(Collectors.toList());
    }

    /**
     * Get all records that changed in the interval
     */
    private PreparedStatement getAllChangedIDsStatement(Timestamp from, Timestamp until, Connection connection)
            throws SQLException
    {
        String sql = "SELECT id, collection, created, deleted, data#>>'{@graph,1,@type}' AS mainEntityType FROM lddb__versions WHERE modified >= ? AND modified <= ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setTimestamp(1, from);
        preparedStatement.setTimestamp(2, until);
        return preparedStatement;
    }

    /**
     * Get all changedBy names for the given id and interval
     */
    private Set<String> getAllChangedBy(String id, Timestamp from, Timestamp until, Connection connection)
            throws SQLException
    {
        HashSet<String> result = new HashSet<>();
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

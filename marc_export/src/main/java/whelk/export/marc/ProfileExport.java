package whelk.export.marc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import whelk.exception.WhelkRuntimeException;
import whelk.util.BlockingThreadPool;
import whelk.util.LegacyIntegrationTools;
import whelk.util.MarcExport;
import whelk.util.ThreadDumper;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
    private final JsonLD2MarcXMLConverter m_toMarcXmlConverter;
    private final Whelk m_whelk;
    private final DataSource m_connectionPool;
    private final BlockingThreadPool m_threadPool;
    
    public ProfileExport(Whelk whelk, DataSource connectionPool)
    {
        m_whelk = whelk;
        m_connectionPool = connectionPool;
        m_threadPool = new BlockingThreadPool(this.getClass().getSimpleName(), Runtime.getRuntime().availableProcessors());
        
        m_toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());

        // _only_ the derivatives, not "Work" itself, as that is what the "classical" MARC works use, which we
        // do not want to filter out as bib.
        workDerivativeTypes = new HashSet<>(m_whelk.getJsonld().getSubClasses("Work"));
    }

    public void shutdown() {
        m_threadPool.shutdown();
    }

    public static class Parameters {
        ExportProfile profile;
        String from;
        String until;
        DELETE_MODE deleteMode;
        boolean doVirtualDeletions;

        ZonedDateTime zonedFrom;
        ZonedDateTime zonedUntil;

        Timestamp fromTimeStamp;
        Timestamp untilTimeStamp;
        
        public Parameters(ExportProfile profile, String from,
                          String until, DELETE_MODE deleteMode, boolean doVirtualDeletions) {
            this.profile = profile;
            this.from = from;
            this.until = until;
            this.deleteMode = deleteMode;
            this.doVirtualDeletions = doVirtualDeletions;

            zonedFrom = ZonedDateTime.parse(from);
            zonedUntil = ZonedDateTime.parse(until);

            // To account for small diffs in between client and server clocks, always add an extra second
            // at the low end of time intervals!
            zonedFrom = zonedFrom.minus(1, ChronoUnit.SECONDS);

            fromTimeStamp = new Timestamp(zonedFrom.toInstant().getEpochSecond() * 1000L);
            untilTimeStamp = new Timestamp(zonedUntil.toInstant().getEpochSecond() * 1000L);
        }
    }
    
    /**
     * Export MARC data affected in between 'from' and 'until' shaped by 'profile' into 'output'.
     * Return a set of IDs that should be deleted separately and the reason why. If deleteMode is not 'SEPARATE', the
     * returned collection will be empty.
     */
    public Map<String, DELETE_REASON> exportInto(MarcRecordWriter output, Parameters parameters)
            throws IOException, SQLException
    {
        if (parameters.profile.getProperty("status", "ON").equalsIgnoreCase("OFF"))
            return Collections.EMPTY_MAP;
        
        ParallelExporter exporter = new ParallelExporter(output, parameters, m_threadPool);
        
        try {
            try (Connection connection = m_connectionPool.getConnection()) {
                connection.setAutoCommit(false);
                try (PreparedStatement preparedStatement = getAllChangedIDsStatement(parameters.fromTimeStamp, parameters.untilTimeStamp, connection);
                     ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        exporter.submit(resultSet);
                    }
                }
            }
            exporter.awaitCompletion();
        }
        finally {
            totalExportCount.observe(exporter.exportedIDs.size());
        }

        return exporter.deletedNotifications;
    }
    
    /**
     * Export (into output) all documents that are affected by 'id' having been updated.
     * 'created' == true means 'id' was created in the chosen interval, false means merely updated.
     */
    private void exportAffectedDocuments(String id, String collection, boolean created, Boolean deleted,Timestamp from,
                                         Timestamp until, ExportProfile profile, MarcRecordWriter output,
                                         DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                         Set<String> exportedIDs, Map<String, DELETE_REASON> deletedNotifications,
                                         String mainEntityType, Connection connection)
            throws IOException, SQLException
    {
        Summary.Timer requestTimer = singleExportLatency.labels(collection).startTimer();
        try
        {
            exportAffectedDocuments2(id, collection, created, deleted, from, until, profile, 
                    output, deleteMode, doVirtualDeletions, exportedIDs, deletedNotifications, mainEntityType, connection);
        }
        finally
        {
            requestTimer.observeDuration();
        }
    }

    private void exportAffectedDocuments2(String id, String collection, boolean created, Boolean deleted,Timestamp from,
                                          Timestamp until, ExportProfile profile, MarcRecordWriter output,
                                          DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                          Set<String> exportedIDs, Map<String, DELETE_REASON> deletedNotifications,
                                          String mainEntityType, Connection connection)
            throws IOException, SQLException
    {
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

            // The 'versions' list is sorted, with the oldest version first.
            // We go through it in reverse (i.e. starting with the newest version).
            // 1. We iterate through the list, skipping (continue) until we've found a
            // version inside the update interval.
            // 2. We keep iterating over versions and check if we're still inside the
            // interval _after_ each iteration, which means we will pass (export) all
            // versions inside the interval and exactly one version "before" the interval.
            for (int i = versions.size()-1; i > -1; --i)
            {
                Document version = versions.get(i);
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
        
        if (usingCollectionRules.equals("auth") && !hasCardChanged(id, from, until)) {
            return false;
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
                return profile.locations().contains(updatedDocument.getHeldBySigel());
            }
        }

        return true;
    }

    private boolean hasCardChanged(String id, Timestamp from, Timestamp until) {
        Document currentVersion = m_whelk.getStorage().loadAsOf(id, until);
        Document previousVersion = m_whelk.getStorage().loadAsOf(id, from);
        if (previousVersion == null) {
            return true;
        }

        var jsonLd = m_whelk.getJsonld();
        var oldCard = jsonLd.toCard(previousVersion.data);
        var newCard = jsonLd.toCard(currentVersion.data);
        return !oldCard.equals(newCard);
    }

    private List<String> getAffectedBibIdsForAuth(String authId, ExportProfile profile) {
        List<String> allIds = m_whelk.getStorage()
                .followDependers(authId, JsonLd.NON_DEPENDANT_RELATIONS)
                .stream().map(Tuple2::getFirst).toList();

        if (allIds.size() > 50000) {
            logger.warn("Unusually large amount of affected IDs ({}) caused by {}", allIds.size(), authId);
        }

        if (profile.shouldExportAllLocations()) {
            return allIds.stream()
                    .filter(id -> "bib".equals(m_whelk.getStorage().getCollectionBySystemID(id)))
                    .collect(Collectors.toList());
        }
        else {
            List<String> locationLibraryUris = locationLibraryUris(profile);
            int batchSize = 5000;
            List<String> result = new java.util.ArrayList<>();
            int numBatches = (allIds.size() + batchSize - 1) / batchSize;

            for (int i = 0; i < numBatches; i++) {
                List<String> batch = allIds.subList(i * batchSize, Math.min((i + 1) * batchSize, allIds.size()));
                result.addAll(m_whelk.getStorage().filterBibIdsByHeldBy(batch, locationLibraryUris));
            }

            return result;
        }
    }

    /**
     * Export bib document (into output)
     */
    private void exportDocument(Document document, ExportProfile profile, MarcRecordWriter output,
                                Set<String> exportedIDs, DELETE_MODE deleteMode, boolean doVirtualDeletions,
                                Map<String, DELETE_REASON> deletedNotifications)
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
        synchronized (exportedIDs) {
            if (exportedIDs.contains(systemId))
                return;
            exportedIDs.add(systemId);
        }
        affectedCount.observe(1);

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
            } catch (IOException | PreviousErrorException e) {
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
        String sql = "SELECT id, collection, created, deleted, data#>>'{@graph,1,@type}' AS mainEntityType FROM lddb__versions WHERE modified >= ? AND modified <= ? AND collection in ('bib', 'auth', 'hold')";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setTimestamp(1, from);
        preparedStatement.setTimestamp(2, until);
        preparedStatement.setFetchSize(10_000); // average size is less than 2kb, default 16 DB connections ~ 320MB
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

    /**
     * Prepare and convert documents to MARC in parallel.
     *
     * Work is performed by a shared pool handling all requests because tasks are CPU bound. 
     * Serializing and writing the converted records to the output stream is done by a separate single thread per request. 
     * 
     * Getting ids to be exported from the database will always be faster than exporting them.
     * Since the task size can vary significantly (auth exports are much larger than bib) we don't want the calling 
     * thread to perform any work (i.e. ThreadPoolExecutor.CallerRunsPolicy) since it might get caught up in a large
     * export and starve the pool threads.
     * So we need to block the calling thread when too much work is queued up.
     *
     * A possible improvement would be to split auth exports into smaller pieces (i.e. parallel exportDocument() instead)
     */
    private class ParallelExporter {
        private final BlockingThreadPool pool;
        
        BlockingThreadPool.Queue workQueue;
        
        Set<String> exportedIDs = new HashSet<>();
        Map<String, DELETE_REASON> deletedNotifications = new ConcurrentHashMap<>();

        Parameters parameters;
        MarcRecordWriterThread out;

        public ParallelExporter(MarcRecordWriter output, Parameters parameters, BlockingThreadPool pool) {
            this.out = new MarcRecordWriterThread(output);
            this.parameters = parameters;
            this.pool = pool;
            this.workQueue = pool.getQueue();
        }
        
        public void submit(ResultSet resultSet) throws SQLException, IOException {
            if (out.hasErrored()) {
                workQueue.cancelAll();
                out.throwIfError();
            }

            workQueue.submit(new Task(resultSet));
        }

        public void awaitCompletion() throws IOException {
            workQueue.awaitAll();
            out.close();
        }

        public void shutdown() {
            pool.shutdown();
        }
        
        private class Task implements Runnable {
            String id;
            String collection;
            String mainEntityType;
            Timestamp createdTime;
            Boolean deleted;
            boolean created;

            public Task(ResultSet resultSet) throws SQLException {
                id = resultSet.getString("id");
                collection = resultSet.getString("collection");
                mainEntityType = resultSet.getString("mainEntityType");
                createdTime = resultSet.getTimestamp("created");
                deleted = resultSet.getBoolean("deleted");
                created = parameters.zonedFrom.toInstant().isBefore(createdTime.toInstant()) &&
                        parameters.zonedUntil.toInstant().isAfter(createdTime.toInstant());
            }

            @Override
            public void run() {
                try (Connection connection = m_whelk.getStorage().getOuterConnection()) {
                    exportAffectedDocuments(id, collection, created, deleted, parameters.fromTimeStamp, 
                            parameters.untilTimeStamp, parameters.profile, out, parameters.deleteMode,
                            parameters.doVirtualDeletions, exportedIDs, deletedNotifications, mainEntityType, connection);
                } catch (PreviousErrorException ignored) {
                    // Already handled elsewhere
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static class PreviousErrorException extends RuntimeException {

    }

    private class MarcRecordWriterThread implements MarcRecordWriter {
        private static final int BUFFER_SIZE = 1000;
        private static final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(MarcRecordWriterThread.class.getSimpleName()).build();

        volatile Exception error;
        
        // The single thread that performs serialization and writes to the actual output stream 
        ThreadPoolExecutor thread = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(BUFFER_SIZE), threadFactory);

        MarcRecordWriter out;

        public MarcRecordWriterThread(MarcRecordWriter out) {
            this.out = out;
        }

        public boolean hasErrored() {
            return error != null;
        }

        @Override
        public void writeRecord(MarcRecord marcRecord) throws IOException {
            if (hasErrored()) {
                throw new PreviousErrorException();
            }

            Runnable task = () -> {
                try {
                    out.writeRecord(marcRecord);
                } catch (Exception e) {
                    if (error == null) {
                        error = e;
                    }
                    thread.shutdownNow();
                }
            };

            int maxTimeouts = 100;
            while (maxTimeouts-- > 0) {
                try {
                    thread.execute(task);
                    return;
                } catch (RejectedExecutionException e) {
                    if (thread.isShutdown()) {
                        return;
                    }
                    // We're producing records faster than can be written (read by client), back off
                    try {
                        logger.info("Slow client, slowing down");
                        Thread.sleep(250);
                    } catch (InterruptedException ignored) {}
                }
            }
            throw new IOException("Reached max timeouts while write queue was full");
        }

        @Override
        public void close() throws IOException {
            thread.shutdown();
            // Await all pending writes
            try {
                int timeout = 5 * 60;
                long p1 = thread.getTaskCount() - thread.getCompletedTaskCount();
                if (!thread.isTerminated() && !thread.awaitTermination(timeout, TimeUnit.SECONDS)) {
                    long p2 = thread.getTaskCount() - thread.getCompletedTaskCount();
                    var msg = String.format("Could not write all pending records (~%s) within %s seconds. ~%s remaining",
                            p1, timeout, p2);
                    logger.warn(msg);
                    logger.warn(ThreadDumper.threadInfo(MarcRecordWriterThread.class.getSimpleName()));
                    throw new IOException(msg);
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            throwIfError();
        }
        
        public void throwIfError() throws IOException {
            if (hasErrored()) {
                if (error instanceof IOException) {
                    throw new IOException(error);
                } else {
                    throw new WhelkRuntimeException("", error);
                }
            }
            
        }
    }
}

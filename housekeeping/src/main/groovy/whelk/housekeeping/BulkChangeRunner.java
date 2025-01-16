package whelk.housekeeping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.datatool.bulkchange.BulkJob;
import whelk.search.ESQuery;
import whelk.search.ElasticFind;

import java.util.List;
import java.util.Map;

import static whelk.datatool.bulkchange.BulkJobDocument.Status.Ready;
import static whelk.datatool.bulkchange.BulkJobDocument.JOB_TYPE;
import static whelk.datatool.bulkchange.BulkJobDocument.STATUS_KEY;

public class BulkChangeRunner extends HouseKeeper {
    private static final Logger logger = LogManager.getLogger(BulkChangeRunner.class);
    private static final ThreadGroup threadGroup = new ThreadGroup(BulkChangeRunner.class.getSimpleName());

    private final String status = "OK";
    private final Whelk whelk;
    private final ElasticFind find;

    public BulkChangeRunner(Whelk whelk) {
        this.whelk = whelk;
        this.find = new ElasticFind(new ESQuery(whelk));
    }

    public String getName() {
        return "BulkChangeRunner";
    }

    public String getStatusDescription() {
        return status;
    }

    public String getCronSchedule() {
        return "11,31,51 * * * *";
    }

    public void trigger() {
        logger.info(BulkChangeRunner.class.getSimpleName());
        try {
            var query = Map.of(
                    JsonLd.TYPE_KEY, List.of(JOB_TYPE),
                    STATUS_KEY, List.of(Ready.key())
            );

            find.findIds(query).forEach(this::run);
        } catch (Exception e) {
            logger.error("Failed checking for new bulk changes: ", e);
            throw new RuntimeException(e);
        }
    }

    private void run(String systemId) {
        String id = Document.getBASE_URI() + systemId + Document.HASH_IT;
        // TODO Improve housekeeping task execution, start, stop etc etc
        new Thread(threadGroup, new BulkJob(whelk, id)).start();
    }
}
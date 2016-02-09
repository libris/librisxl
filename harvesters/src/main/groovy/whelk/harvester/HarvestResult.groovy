package whelk.harvester

/**
 * Created by markus on 2016-02-08.
 */
class HarvestResult {
    // Start values
    Date fromDate
    Date untilDate
    // Result values
    int numberOfDocuments
    int numberOfDocumentsDeleted
    int numberOfDocumentsSkipped
    // in-flight data
    String resumptionToken
    Date lastRecordDatestamp

    HarvestResult() {
        setFromDate(null)
    }

    HarvestResult(Date from) {
        setFromDate(from)
    }
    HarvestResult(Date from, Date until) {
        setFromDate(from)
        untilDate = until
    }

    /**
     * Set lastRecordDate to same as from to make sure it is always set, even if first record crashes on DateInconsistency
     * @param d
     */
    void setFromDate(Date d) {
        if (d == null) {
            d = new Date(0L)
        }
        fromDate = new Date(d.getTime())
        lastRecordDatestamp = new Date(d.getTime())
    }
}

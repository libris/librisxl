package whelk.importer

/**
 * Created by theodortolstoy on 2017-01-05.
 */
class ImportResult {
    // Start values
    Date fromDate
    Date untilDate
    // Result values
    int numberOfDocuments = 0
    int numberOfDocumentsDeleted = 0
    int numberOfDocumentsSkipped = 0
    // in-flight data
    String resumptionToken = null
    private Date lastRecordDatestamp
    String sourceSystem = null

    ImportResult() {
        setFromDate(null)
    }

    ImportResult(Date from, String srcSys) {
        this.sourceSystem = srcSys
        setFromDate(from)
    }
    ImportResult(Date from, Date until, String srcSys) {
        this.sourceSystem = srcSys
        setFromDate(from)
        if (until != null) {
            untilDate = new Date(until.getTime())
        }
    }

    Date getLastRecordDatestamp() {
        return new Date(lastRecordDatestamp.getTime())
    }

    void setLastRecordDatestamp(Date lrds) {
        this.lastRecordDatestamp = new Date(lrds.getTime())
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

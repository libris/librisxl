package whelk.harvester

class HarvestResult {
    int numberOfDocuments
    int numberOfDeleted
    int numberOfDocumentsSkipped
    String resumptionToken
    Date lastRecordDatestamp
}

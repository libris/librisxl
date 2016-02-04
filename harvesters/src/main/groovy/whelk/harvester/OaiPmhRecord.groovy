package whelk.harvester

/**
 * Created by markus on 2016-02-03.
 */
class OaiPmhRecord {
    String record
    String identifier
    String datestamp
    List<String> setSpecs = new ArrayList<String>()
    boolean deleted = false
}

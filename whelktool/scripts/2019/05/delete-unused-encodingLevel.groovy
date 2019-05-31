/*
 * This script sets obsolete or OCLC specfic encodingLevels to marc:AbbreviatedLevel
 *
 * See LXL-2546 for more info.
 *
 */

NEW_ENCODING_LEVEL_VALUE = "marc:AbbreviatedLevel"
PrintWriter scheduledToUpdateEncodingLevel = getReportWriter("scheduled-to-update-encodingLevel")

selectBySqlWhere("""
        collection in ('bib') AND
        data#>>'{@graph,0,encodingLevel}' = 'marc:FullLevelInputByOclcParticipantsLocal' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:LessThanFullLevelInputAddedFromABatchProcessLocal' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:FullLevelInputAddedFromABatchProcessLocal' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:LessThanFullLevelInputByOclcParticipantsLocal' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:DeletedRecordLocal' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:CoreLevel'
    """) { data ->

    def (record, instance, work) = data.graph

    record["encodingLevel"] = NEW_ENCODING_LEVEL_VALUE
    scheduledToUpdateEncodingLevel.println("${record[ID]}")
    data.scheduleSave()
}
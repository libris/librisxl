/*
 * This changes the Work subclass type of ProjectedImage to MovingImage
 *
 * See LXL-2601 for more info.
 *
 */

String INCORRECT_TYPE = 'ProjectedImage'
String CORRECT_TYPE = 'MovingImage'
PrintWriter scheduledToUpdate = getReportWriter("scheduled-to-update-projectedImage-type-to-movingImage")

selectBySqlWhere("""
        data#>>'{@graph,2,@type}' = '${INCORRECT_TYPE}' AND collection = 'bib' OR
        data#>>'{@graph,2,hasPart}' LIKE  '%"@type": "${INCORRECT_TYPE}"%' AND collection = 'bib'
    """) { data ->

    def (record, instance, work) = data.graph

    if (work[TYPE] == INCORRECT_TYPE) {
            if (work[TYPE] == INCORRECT_TYPE)
            work[TYPE] = CORRECT_TYPE
        scheduledToUpdate.println("${record[ID]}")
        data.scheduleSave()
    }
    work.hasPart?.each {
      if (it[TYPE] == INCORRECT_TYPE) {
              if (it[TYPE] == INCORRECT_TYPE)
              it[TYPE] = CORRECT_TYPE
          scheduledToUpdate.println("${record[ID]}")
          data.scheduleSave()
      }
    }

}

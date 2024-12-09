/*
  A whelktool script (not checked in) was executed in an unfortunate way,
  causing a number of records to have "/./" in their generationProcess.@id URI,
  which Jena's IRI validator didn't like, making it impossible to save the records
  via (at least) the REST API.

  We fix it by simply re-saving them.

  See LXL-4548 for more info.
*/

selectByIds(new File(scriptDir, "ids.txt").readLines()) {
  it.scheduleSave()
}

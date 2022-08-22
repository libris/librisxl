/**
 * The script for LXL-3705 that was used for linking bibliographies that have been corrected in BibDB
 * (lxl-2469-link-bibliographies.groovy) did not save them loud. Re-save loud to trigger exports.
 */

String idsFile = System.getProperty('ids')

selectByIds(new File(idsFile).readLines()) { data ->
    data.scheduleSave(loud: true)
}
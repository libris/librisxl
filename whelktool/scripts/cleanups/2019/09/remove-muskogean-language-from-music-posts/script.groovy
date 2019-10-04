/*
 * Remove https://id.kb.se/language/mus from all posts that are not really Muskogean
 *
 * 2435 posts with https://id.kb.se/language/mus (muskogean-ids.txt)
 * 2405 of those have type "NotatedMusic" or "Music"
 * 29 of the remaining 30 are not Muskogean through manual inspection of titles (verified-not-muskogean-ids.txt)
 *
 * Any "Music" or "NotatedMusic" that is actually in Muskogean will have language removed...
 *
 * ID list generated with
 * curl '10.50.16.190:9200/libris_stg/_search?type=bib&q=(Muskogeanska)&size=3000&filter_path=hits.total,hits.hits._id&pretty=true' | grep _id | tr '"' ' ' | awk '{print $3}' > muskogeanska-ids.txt
 *
 * See LXL-2443 for more info.
 *
 */
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

File bibIds = new File(scriptDir, 'muskogean-ids.txt')
verifiedNotMuskogean = new File(scriptDir, 'verified-not-muskogean-ids.txt').readLines()

selectByIds(bibIds.readLines()) { bib ->
    if (isNotMuskogean(bib)) {
        if (removeMuskogean(bib.doc.data)) {
            scheduledForUpdate.println("${bib.doc.getURI()}")
            bib.scheduleSave()
        }
    }
    else {
        println ("MUSKOGEAN?: ${bib.doc.getURI()} ${bib.doc.data["@graph"][1]['hasTitle']}")
    }
}

boolean isNotMuskogean(bib) {
    return isMusic(bib) || bib.doc.shortId in verifiedNotMuskogean
}

boolean isMusic(bib) {
    def data = bib.doc.data
    def workType = data["@graph"][2]["@type"]

    return workType in ["NotatedMusic", "Music"]
}

boolean removeMuskogean(bibData) {
    Map work = bibData["@graph"][2]
    return removeFromListProperty(work, "language", ["@id": "https://id.kb.se/language/mus"])
}

boolean removeFromListProperty(Map data, String propertyName, Object toBeRemoved) {
    List property = data[propertyName]
    if (property != null) {
        if (property.remove(toBeRemoved)) {
            if (property.isEmpty()) {
                data.remove(propertyName)
            }
            return true
        }
    }
    return false
}
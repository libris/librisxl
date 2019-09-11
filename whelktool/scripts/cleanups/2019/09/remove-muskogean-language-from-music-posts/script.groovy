/*
 * This removes https://id.kb.se/language/mus from all posts that are not really Muskogean
 *
 * 2435 posts with https://id.kb.se/language/mus
 * 2405 of those have type "NotatedMusic" or "Music"
 * Manual inspection of the titles of the 30 remaining gave one post with actual language Muskogean
 *
 * This has the problem of removing Muskogean from all "Music" or "NotatedMusic" that is actually in Muskogean...
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
        removeMuskogean(bib.doc.data)
        scheduledForUpdate.println("${bib.doc.getURI()}")
        bib.scheduleSave()
    }
    else {
        println ("MUSKOGEAN?: ${bib.doc.getURI()} ${bib.doc.data["@graph"][1]['hasTitle']}")
    }
}

boolean isNotMuskogean(bib) {
    return isMusic(bib) || verifiedNotMuskogean.contains(bib.doc.getURI().toString())
}

boolean isMusic(bib) {
    def data = bib.doc.data
    def workType = data["@graph"][2]["@type"]

    return workType in ["NotatedMusic", "Music"]
}

void removeMuskogean(bibData) {
    List languages = bibData["@graph"][2]["language"]
    languages.remove(["@id": "https://id.kb.se/language/mus"])
}
PrintWriter failedHoldIDs  = getReportWriter("Failed-to-create-holds.txt")
PrintWriter failedModified = getReportWriter("Failed-to-modify-bibs.txt")

File input = new File(scriptDir, "littbanken_hold_link.csv")
List<String> programLines = input.readLines().drop(1) // skip header row

// --- Helper to normalize Swedish letters and strip https:// ---
String normalizeUrl(String s) {
    if (!s) return ""
    return s
        .replaceFirst("^https://", "")
        .replaceAll("[åä]", "a")
        .replaceAll("ö", "o")
        .trim()
}

// --- Build a map of LibrisID -> metadata from CSV ---
Map<String, Map> metadataById = [:]

programLines.each { line ->
    if (!line.trim()) return // skip empty rows
    String[] part = line.split(';', -1)
    if (part.size() < 3) {
        println "Skipping short row: ${line}"
        return
    }

    String librisCN = part[0].trim()
    String librisID = part[1].trim()
    String url      = part[2].trim()

    metadataById[librisID] = [
        librisCN : librisCN,
        url      : url,
        shortUrl : url.replaceFirst("^https://", "").trim(),
        normUrl  : normalizeUrl(url)
    ]
}

// --- Process all bibs by IDs ---
selectByIds(metadataById.keySet() as List) { bib ->
    def instance = bib.graph[1]
    String bibId = bib.doc.shortId
    def meta     = metadataById[bibId]
    if (!meta) return // skip if ID not in CSV

    boolean foundUsage = false
    boolean foundUrl   = false

    // usageAndAccessPolicy
    instance["usageAndAccessPolicy"] = asList(instance["usageAndAccessPolicy"])
    instance["usageAndAccessPolicy"].each { pol ->
        asList(pol["uri"]).each { uri ->
            if (uri instanceof String &&
                uri.contains("litteraturbanken.se/om/rattigheter")) {
                foundUsage = true
            }
        }
    }

    // associatedMedia
    instance["associatedMedia"] = asList(instance["associatedMedia"])
    instance["associatedMedia"].each { media ->
        asList(media["uri"]).each { uri ->
            if (!(uri instanceof String)) return
            if (uri.contains(meta.shortUrl) || uri.contains(meta.normUrl)) {
                foundUrl = true
            }
        }
    }

    // Add missing values
    if (!foundUsage) {
        instance["usageAndAccessPolicy"] << [
            "@type": "UsePolicy",
            "label": "Rättigheter för Litteraturbankens texter",
            "uri"  : ["http://litteraturbanken.se/om/rattigheter"]
        ]
    }
    if (!foundUrl) {
        instance["associatedMedia"] << [
            "uri"            : [meta.url],
            "@type"          : "MediaObject",
            "cataloguersNote": ["856free"],
            "marc:publicNote": "Fritt tillgänglig via Litteraturbankens webbplats"
        ]
    }

    // Save
    bib.scheduleSave(loud: true, onError: { e ->
        failedModified.println("Failed to update ${bibId} due to: $e")
    })
}

// Helper to always return a list
private List asList(Object o) {
    (o instanceof List) ? o : (o ? [o] : [])
}

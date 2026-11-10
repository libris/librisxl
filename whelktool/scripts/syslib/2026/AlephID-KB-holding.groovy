File bibids = new File(scriptDir, "AlephID-nov26.txt")

Map<String, String> holdToAleph = [:]

for (String line : bibids.readLines()) {
    if (line.isEmpty()) {
        continue
    }

    String[] part = line.split('\t')

    if (part.length < 4 || part[3].trim().isEmpty()) {
        continue
    }

    String alephId  = part[0].trim()
    String holdUrl  = part[3].trim()

    holdToAleph[holdUrl] = alephId
}

selectByIds(holdToAleph.keySet().toList()) { hold ->

    String holdUrl = hold.doc.getURI().toString() + "#it"
    String alephId = holdToAleph[holdUrl]

    if (!alephId) {
        return
    }

    Map identifier = [
        "@type": "SystemNumber",
        "value": "(Aleph)${alephId}"
    ]

    if (!hold.graph[0]["identifiedBy"]) {
        hold.graph[0]["identifiedBy"] = []
    }

    hold.graph[0]["identifiedBy"] << identifier

    hold.scheduleSave(loud: true)
}

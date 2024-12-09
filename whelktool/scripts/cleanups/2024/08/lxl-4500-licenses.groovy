String where = "collection = 'bib' and deleted = false and data#>>'{@graph,1,usageAndAccessPolicy}' like '%creativecommons.org%' "

selectBySqlWhere(where) { data ->
    Map instance = data.graph[1]

    if (! instance["usageAndAccessPolicy"] instanceof List)
        instance.put("usageAndAccessPolicy", [instance["usageAndAccessPolicy"]])

    //System.out.println("BEFORE: ${instance["usageAndAccessPolicy"]}")

    boolean changed = false
    instance["usageAndAccessPolicy"].each { usage ->

        if (usage["uri"] != null) {
            usage["uri"].removeAll { uri ->
                String licenseId = getCorrectLicense(uri)
                if (licenseId != null) {
                    usage.clear()
                    usage.put("@id", uri)
                    changed = true
                    return true
                }
                return false
            }
        }
    }

    if (changed) {
        data.scheduleSave()
        //System.out.println("AFTER: ${instance["usageAndAccessPolicy"]}\n")
    }
}

String getCorrectLicense(String dirtyLicense) {

    switch (dirtyLicense) {

        // Known common but (currently) ignored licenses:

        case "https://creativecommons.org/licenses/by-nc-nd/3.0/de":
        case "https://creativecommons.org/licenses/by-nc-nd/3.0":
        case "http://creativecommons.org/licenses/by-nc-sa/3.0/de/":
        case "http://creativecommons.org/licenses/by-nc-nd/3.0/de/":
        case "http://creativecommons.org/licenses/by/2.5/se/":
        case "http://creativecommons.org/licenses/by-nc-nd/2.5/se/":
            return null

        // Licences we want to link:

        case "http://creativecommons.org/licenses/by/4.0":
        case "https://creativecommons.org/licenses/by/4.0/":
        case "https://creativecommons.org/licenses/by/4.0":
        case "https://creativecommons.org/licenses/by/4.0/deed.de":
            return "https://creativecommons.org/licenses/by/4.0/"

        case "http://creativecommons.org/licenses/by-nc-nd/4.0":
        case "https://creativecommons.org/licenses/by-nc-nd/4.0/":
        case "https://creativecommons.org/licenses/by-nc-nd/4.0":
        case "https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode":
        case "https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode.de":
            return "https://creativecommons.org/licenses/by-nc-nd/4.0/"

        case "http://creativecommons.org/licenses/by%2Dnc%2Dsa/4.0":
        case "https://creativecommons.org/licenses/by-nc-sa/4.0/":
            return "https://creativecommons.org/licenses/by-nc-sa/4.0/"

        case "https://creativecommons.org/licenses/by-nd/4.0/":
        case "https://creativecommons.org/publicdomain/mark/1.0/":
            return dirtyLicense
    }
    return null
}
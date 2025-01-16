String where = "collection = 'bib' and deleted = false and " +
        "(" +
        "  data#>>'{@graph,1,usageAndAccessPolicy}' like '%https://creativecommons.org/publicdomain/zero/1.0/%' or " +
        "  data#>>'{@graph,1,usageAndAccessPolicy}' like '%https://creativecommons.org/publicdomain/mark/1.0/deed.sv%' " +
        ") "

selectBySqlWhere(where) { data ->
    Map instance = data.graph[1]

    if (! instance["usageAndAccessPolicy"] instanceof List)
        instance.put("usageAndAccessPolicy", [instance["usageAndAccessPolicy"]])

    boolean removed = instance["usageAndAccessPolicy"].removeAll { usage ->
        return usage["uri"] != null && usage["@type"] != null &&
        ( usage["uri"] == ["https://creativecommons.org/publicdomain/mark/1.0/deed.sv"] || usage["uri"] == ["https://creativecommons.org/publicdomain/zero/1.0/"] ) &&
        usage["@type"] == "UsePolicy"
    }

    if (removed) {
        instance["usageAndAccessPolicy"].add(["@id" : "https://creativecommons.org/publicdomain/mark/1.0/"])
        data.scheduleSave()
        //System.out.println("After fixing: ${instance["usageAndAccessPolicy"]}")
    }
}

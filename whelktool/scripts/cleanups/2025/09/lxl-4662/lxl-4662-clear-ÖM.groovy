def ids = new File(scriptDir, 'ids.txt').readLines()

ids.each {id ->

    String where = null
    if (id.startsWith("(LIBRIS)")) { // libris control number
        String controlnumber = id.substring(8);
        where = "id in (\n" +
                "select h.id from\n" +
                "lddb b\n" +
                "left join lddb h on h.data#>>'{@graph,1,itemOf,@id}' = b.data#>>'{@graph,1,@id}'\n" +
                "where b.collection = 'bib' and b.data#>>'{@graph,0,controlNumber}' = '" + controlnumber + "'  and h.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/G'\n" +
                ")"

    } else { // ISBN
        where = "id in (\n" +
                "select h.id from\n" +
                "lddb b\n" +
                "left join lddb h on h.data#>>'{@graph,1,itemOf,@id}' = b.data#>>'{@graph,1,@id}'\n" +
                "where b.collection = 'bib' and b.data#>'{@graph,1,identifiedBy}' @> '[{\"@type\":\"ISBN\", \"value\":\"" + id + "\"}]'  and h.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/G'\n" +
                ")"
    }

    selectBySqlWhere(where) { doc ->
        boolean changed = false

        changed |= clearÖmFromShelfMark(doc.graph[1]?.shelfMark)
        if (!doc.graph[1]?.shelfMark?.containsKey("label"))
            doc.graph[1].remove("shelfMark")

        if (doc.graph[1]?.hasComponent) {
            doc.graph[1]?.hasComponent.each { component ->
                if (component.shelfMark) {
                    changed |= clearÖmFromShelfMark(component.shelfMark)

                    if (!component.shelfMark.containsKey("label"))
                        component.remove("shelfMark")
                }
            }
        }

        if (changed) {
            doc.scheduleSave()
        }
    }
}

boolean clearÖmFromShelfMark(Map shelfMark) {

    //def original = shelfMark?.label

    if (shelfMark?.label) {
        if (shelfMark.label instanceof List) {
            List<String> newLabels = []
            for (String label : shelfMark.label) {
                if (label.contains("ÖM")) {
                    String newLabel = label.replaceAll("ÖM", "").trim()
                    if (newLabel.length() > 0)
                        newLabels.add(newLabel)
                } else {
                    newLabels.add(label)
                }
            }
            if (newLabels.size() == 0) {
                shelfMark.remove("label")
                //System.err.println("" + original + " -> " + shelfMark)
                return true
            }
            else if (!newLabels.equals(shelfMark.label)) {
                shelfMark.label = newLabels
                //System.err.println("" + original + " -> " + shelfMark)
                return true
            }
            return false
        } else {
            String newShelfMark = shelfMark.label.replaceAll("ÖM", "").trim()
            if (newShelfMark == "") {
                shelfMark.remove("label")
            }
            else if (newShelfMark != shelfMark.label) {
                shelfMark.label = newShelfMark
                //System.err.println("" + original + " -> " + shelfMark)
                return true
            }
        }
    }
    return false
}

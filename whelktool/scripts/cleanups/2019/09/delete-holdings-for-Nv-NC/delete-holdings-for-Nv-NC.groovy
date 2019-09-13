/*
 * This deletes holdings for sigel Nv with shelfMark or physicalLocation starting with "NC"
 *
 * See LXL-2344 for more info.
 *
 */

failedHoldIDs = getReportWriter("failed-holdIDs")
scheduledForDeletion = getReportWriter("scheduled-for-deletion")
scheduledForUpdate = getReportWriter("scheduled-for-update")

where = """
collection = 'hold'
and data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Nv'
"""

selectBySqlWhere(where, silent: false) { hold ->
    Map item = hold.doc.data['@graph'][1]
    if(isNcShelf(item)) {
        delete(hold)
    }
    else if (item['hasComponent']) {
        List components = item['hasComponent']
        if (components.removeAll(this.&isNcShelf)) {
            if (components.size() == 1) {
                try {
                    promoteLonelyComponent(item)
                    save(hold)
                }
                catch (Exception e) {
                    failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
                }
            }
            else if (components.isEmpty()) {
                delete(hold)
            }
            else {
                save(hold)
            }
        }
    }
}


void promoteLonelyComponent(Map item) {
    Map component = item['hasComponent'].first()
    for (key in component.keySet()) {
        if (item.containsKey(key) && item[key] != component[key]) {
            throw new RuntimeException("Top-level Item already has key with different value: " +
                    "item[key]=${item[key]} component[key]=${component[key]}")
        }
        item[key] = component[key]
    }
    item.remove('hasComponent')
}

boolean isNcShelf(holdItem) {
    (holdItem['shelfMark'] && holdItem['shelfMark']['label'].toString().startsWith('NC')) ||
            (holdItem['physicalLocation']
                    && holdItem['physicalLocation'].size() == 1
                    && holdItem['physicalLocation'][0].startsWith('NC')
            )

}

void delete(hold) {
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
}

void save(hold) {
    scheduledForUpdate.println("${hold.doc.getURI()}")
    hold.scheduleSave()
}
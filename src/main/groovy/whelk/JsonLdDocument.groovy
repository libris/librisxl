package whelk

import groovy.util.logging.Slf4j as Log

@Log
class JsonLdDocument extends JsonDocument {


    final static String ID_KEY = "@id"

    protected List<String> findLinks(Map dataMap) {
        List<String> ids = []
        for (entry in dataMap) {
            if (entry.key == ID_KEY && entry.value != identifier) {
                ids.add(entry.value)
            } else if (entry.value instanceof Map) {
                ids.addAll(findLinks(entry.value))
            } else if (entry.value instanceof List) {
                for (l in entry.value) {
                    if (l instanceof Map) {
                        ids.addAll(findLinks(l))
                    }
                }
            }
        }
        return ids
    }

    void setData(byte[] data) {
        super.setData(data)
        def links = findLinks(getDataAsMap())
        //log.info("For ${identifier}, found links: ${links}")
        if (links) {
            this.entry['links'] = links
        }
    }
}

/**
 Replace local subjects with linked subjects in MTM records
 
 To run: put Librisid_and_Subject_To_Insert.csv and Librisid_and_Subject_To_Delete.csv in whelktool/../..
 
 Example: 
 Librisid_and_Subject_To_Insert.csv
 LibrisId;Subject
 https://libris.kb.se/07vsbrfkxftt2k4m#it;https://id.kb.se/term/barn/Datorer
 https://libris.kb.se/07vsbrfkxftt2k4m#it;https://id.kb.se/term/barn/Datorspel
 https://libris.kb.se/07vsbrfkxftt2k4m#it;https://id.kb.se/term/barn/G%C3%A5tor
 https://libris.kb.se/07vsbrfkxftt2k4m#it;https://id.kb.se/term/barn/Internatskolor

 Librisid_and_Subject_To_Delete.csv
 librisid;label
 https://libris.kb.se/07wmrkfxxwm15qmq#it;"Skolan"
 https://libris.kb.se/07wmrkfxxwm15qmq#it;"Vardagsliv"
 
 See LXL-4247
 */
Map<String, List<Map>> insert = [:]
new File('../../Librisid_and_Subject_To_Insert.csv').readLines().each {
    // Example
    // https://libris.kb.se/07vsbrfkxftt2k4m#it;https://id.kb.se/term/barn/Datorer
    def (id, subject) = it.split(';') as List
    List subjects = insert.getOrDefault(id, [])
    subjects.add(['@id': subject])
    insert[id] = subjects
}

Map<String, List<Map>> delete = [:]
new File('../../Librisid_and_Subject_To_Delete.csv').readLines().each {
    // Example
    // https://libris.kb.se/07wmrkfxxwm15qmq#it;"Skolan"
    def (id, label) = it.split(';') as List
    label = label.replaceAll('"', '') // There are no quotes inside the labels
    List subjects = delete.getOrDefault(id, [])
    subjects.add(['@type': 'Topic', 'label' : [label]])
    subjects.add(['@type': 'Topic', 'label' : label])
    delete[id] = subjects
}

selectByIds(insert.keySet() + delete.keySet()) { doc ->
    def (_, thing) = doc.graph
    if (!thing.instanceOf) {
        return
    }

    (asList(thing.instanceOf.subject) as Set).with {
        it.addAll(insert.getOrDefault(thing['@id'], []))
        it.removeAll(delete.getOrDefault(thing['@id'], []))
        thing.instanceOf.subject = it
    }
    
    doc.scheduleSave()
}
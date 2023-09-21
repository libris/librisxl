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

 Libris_deletes_with_label.csv
 librisid;nodeToDelete;labelToDelete;BecauseReplacedWith
 https://libris.kb.se/07wmrkfxxwm15qmq#it;nodeID://b928575925;Skolan;https://id.kb.se/term/barn/Skolan
 https://libris.kb.se/07wmrkfxxwm15qmq#it;nodeID://b928575926;Vänskap;https://id.kb.se/term/barn/V%C3%A4nskap
 https://libris.kb.se/084dfkwvx5c7430v#it;nodeID://b624433371;Att vara annorlunda;https://id.kb.se/term/barn/Vara%20annorlunda

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
new File('../../Libris_deletes_with_label.csv').readLines().each {
    // Example
    // https://libris.kb.se/07wmrkfxxwm15qmq#it;nodeID://b928575925;Skolan;https://id.kb.se/term/barn/Skolan
    def (id, _, label) = it.split(';') as List
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
    
    doc.scheduleSave(loud: true)
}
package whelk;

import java.util.*;

public class WorkMerging {

    public enum WRITE_RESULT {
        ALREADY_UP_TO_DATE,
        UPDATED,
        CREATED
    }

    // No proper pointers or multiple return values in Java :(
    private static class WriteResultReference {
        public WRITE_RESULT result = WRITE_RESULT.ALREADY_UP_TO_DATE;
    }

    /**
     * Merge the works of all listed instances into one. The listed instances
     * may or may not have external works already. Orphaned work records will be
     * deleted. Extra (previously unsaved) works may optionally be supplied.
     *
     * This is _not_ one atomic operation, but rather a series of operations.
     * This means that it is possible to observe the process halfway though from the
     * outside. It also means that should the process be stopped halfway through,
     * results may look odd (but will still obey basic data integrity rules).
     */
    public static WRITE_RESULT mergeWorksOf(List<String> instanceIDs, List<Document> extraWorks, Whelk whelk) {

        WriteResultReference result = new WriteResultReference();

        List<Document> instances = collectInstancesOfThisWork(instanceIDs, whelk);

        Document baseWork = selectBaseWork(instances, extraWorks, result, whelk);
        String baseWorkUri = baseWork.getThingIdentifiers().get(0);
        Map correctLinkEntity = new HashMap();
        correctLinkEntity.put("@id", baseWorkUri);

        // Collect all already existing external works (different from our target) before relinking
        List<String> orphanIDs = new ArrayList<>();
        for (Document instance : instances) {
            Map workEntity = instance.getWorkEntity();
            if (workEntity.size() == 1 && !workEntity.equals(correctLinkEntity)) {
                String workUri = (String) workEntity.get("@id");
                String workId = whelk.getStorage().getSystemIdByIri(workUri);
                orphanIDs.add(workId);
            }
        }

        // Merge other works into the baseWork. This must be done first, before any orphans can be deleted,
        // or we risk loosing data if the process is interrupted.
        /*whelk.storeAtomicUpdate(baseWork.getShortId(), true, false, true, "xl", null, (Document doc) -> {
            // TODO MERGE HERE AND DONT FORGET TO SET result.result IF ANYTHING CHANGES!
        });*/

        // Relink the instances
        for (Document instance : instances) {
            if (!instance.getWorkEntity().equals(correctLinkEntity)) { // If not already linked to the correct record
                whelk.storeAtomicUpdate(instance.getShortId(), true, false, true, "xl", null, (Document doc) -> {
                    doc.setWorkEntity(correctLinkEntity);
                });
            }
        }

        // Cleanup no longer linked work records
        for (String orphanID : orphanIDs) {
            try {
                whelk.getStorage().removeAndTransferMainEntityURIs(orphanID, baseWork.getShortId());
            } catch (RuntimeException e) {
                // Expected possible cause of exception: A new link was added to this work, _after_ we collected
                // and relinked the instances of it. In this (theoretical) case, just leave the old work in place.
            }
        }

        return result.result;
    }

    /**
     * Find the set of instances that should link to the merged work. This of course includes the
     * passed instanceIDs, but also any other instances already sharing a work with one of those IDs.
     */
    private static List<Document> collectInstancesOfThisWork(List<String> instanceIDs, Whelk whelk) {
        List<Document> instances = new ArrayList<>(instanceIDs.size());
        for (String instanceID : instanceIDs) {
            Document instance = whelk.getDocument(instanceID);
            instances.add( instance );

            // Are there other instances linking to the same work as 'instance' ? If so add them to the
            // collection to (possibly) re-link as well.
            Map workEntity = instance.getWorkEntity();
            if (workEntity.size() == 1 && workEntity.containsKey("@id")) {
                String workUri = (String) workEntity.get("@id");
                String workId = whelk.getStorage().getSystemIdByIri(workUri);
                for (String otherInstanceId : whelk.getStorage().getDependers(workId)) {
                    Document otherInstance = whelk.getDocument(otherInstanceId);
                    instances.add( otherInstance );
                }
            }
        }
        return instances;
    }

    /**
     * Select (or create+save) a work record that should be used going forward for
     * all of the passed instances.
     */
    private static Document selectBaseWork(List<Document> instances, List<Document> extraWorks, WriteResultReference result, Whelk whelk) {
        // Find all the works
        List<String> linkedWorkURIs = new ArrayList<>();
        List<Map> embeddedWorks = new ArrayList<>();
        for (Document instance : instances) {
            Map workEntity = instance.getWorkEntity();
            if (workEntity.size() == 1 && workEntity.containsKey("@id")) {
                linkedWorkURIs.add( (String) workEntity.get("@id"));
            } else {
                embeddedWorks.add(workEntity);
            }
        }

        // Order of priority:
        // 1. Any pre existing linked work records
        // 2. Any supplied extra works
        // 3. Any embedded work from one of the instances

        // Pick a linked one if any such exist (1)
        String baseWorkUri = null;
        if (!linkedWorkURIs.isEmpty()) {
            baseWorkUri = linkedWorkURIs.get(0); // TODO: Be a little smarter about _which_ work we pick?
        } else if(!extraWorks.isEmpty()) { // Any supplied extra work (2)
            Document selectedWork = extraWorks.get(0);

            ((Map)(((List)selectedWork.data.get("@graph")).get(1))).remove("@reverse"); // ugh

            whelk.createDocument(selectedWork, "xl", null, "auth", false);
            result.result = WRITE_RESULT.CREATED;
            baseWorkUri = selectedWork.getThingIdentifiers().get(0);
        } else { // Otherwise break off an embedded one (3)
            String slug = IdGenerator.generate();
            String recordId = Document.getBASE_URI().toString() + slug;
            String mainEntityId = recordId + "#it";

            Map chosenEmbedded = embeddedWorks.get(0); // TODO: Be a little smarter about _which_ work we break off?

            Map docMap = new HashMap();
            List graph = new ArrayList();
            Map record = new HashMap();
            docMap.put("@graph", graph);

            graph.add(record);
            record.put("@id", Document.getBASE_URI().toString() + slug);
            record.put("@type", "Record");
            Map mainEntityLink = new HashMap();
            mainEntityLink.put("@id", mainEntityId);
            record.put("mainEntity", mainEntityLink);

            graph.add(chosenEmbedded);
            chosenEmbedded.put("@id", mainEntityId);

            Document newWork = new Document(docMap);
            newWork.setControlNumber(slug);
            newWork.setGenerationDate(new Date());
            //newWork.setGenerationProcess("https://id.kb.se/workmerger"); // TODO: KOLLA MED FORMAT!!
            whelk.createDocument(newWork, "xl", null, "auth", false);
            result.result = WRITE_RESULT.CREATED;
            baseWorkUri = mainEntityId;
        }

        return whelk.getStorage().loadDocumentByMainId(baseWorkUri);
    }
}

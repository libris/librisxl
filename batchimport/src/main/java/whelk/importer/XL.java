package whelk.importer;

import groovy.lang.Tuple;
import io.prometheus.client.Counter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.Field;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Subfield;
import se.kb.libris.util.marc.impl.MarcRecordImpl;
import se.kb.libris.utils.isbn.ConvertException;
import se.kb.libris.utils.isbn.Isbn;
import se.kb.libris.utils.isbn.IsbnException;
import se.kb.libris.utils.isbn.IsbnParser;
import whelk.Document;
import whelk.IdGenerator;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;
import whelk.converter.MarcJSONConverter;
import whelk.converter.marc.MarcFrameConverter;
import whelk.exception.CancelUpdateException;
import whelk.exception.TooHighEncodingLevelException;
import whelk.filter.LinkFinder;
import whelk.history.History;
import whelk.util.LegacyIntegrationTools;
import whelk.util.PropertyLoader;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import groovy.json.StringEscapeUtils; // Not fun, relying on Groovy. Adding a dependency on Apache Commons may be worse though. Making own implementations are frowned upon :(

import static whelk.util.Jackson.mapper;

class XL
{
    private final Logger logger = LogManager.getLogger(this.getClass());
    public static final String ENC_PRELIMINARY_STATUS = "marc:PartialPreliminaryLevel"; // 5
    public static final String ENC_PREPUBLICATION_STATUS = "marc:PrepublicationLevel";  // 8
    public static final String ENC_ABBREVIVATED_STATUS = "marc:AbbreviatedLevel";  // 3
    public static final String ENC_MINMAL_STATUS = "marc:MinimalLevel";  // 7

    // This is not pretty, but the alternatives are worse. The way this works is that the incoming data arrives in
    // a sequence consisting of ONE bib, followed by zero or more holdings, which is then repeated for the next record and so on.
    // Each record (bibs and holds alike) gets a call to the importISO2709() function, which in the bib-case will return
    // the resulting bib id (which may be a pre-existing record, or one just created). That bib-ID will then be what the
    // holdings that follow will attach to (the hold calls will return null, signalling that the current bib-ID is still what applies).
    //
    // Now here is the crux: There is (now) a case, where we want to *discard* holdings (where the bib didn't match, and we
    // *don't* want to create a new one, because reasons).
    // We can't just return null (this would just cause the next hold to attach to whatever the previous bib-ID was).
    // Instead, we return this bogus bib-id which we will then keep an eye out for, knowing that any holding that wants
    // to attach to it is to be discarded.
    private static final String DISCARD_ATTACHED_HOLDINGS_MARKER = "__DISCARD HOLDINGS UNTIL NEXT BIB ARRIVES";

    private final Whelk m_whelk;
    private final LinkFinder m_linkfinder;
    private final Parameters m_parameters;
    private final Properties m_properties;
    private final MarcFrameConverter m_marcFrameConverter;
    private Merge m_merge;
    private static boolean verbose = false;

    // The predicates listed here are those that must always be represented as lists in jsonld, even if the list
    // has only a single member.
    private final Set<String> m_repeatableTerms;

    private final String IMPORT_SYSTEM_CODE;

    XL(Parameters parameters) throws IOException
    {
        m_parameters = parameters;
        verbose = m_parameters.getVerbose();
        m_properties = PropertyLoader.loadProperties("secret");
        m_whelk = Whelk.createLoadedSearchWhelk(m_properties);
        m_repeatableTerms = m_whelk.getJsonld().repeatableTerms;
        m_marcFrameConverter = m_whelk.getMarcFrameConverter();
        m_linkfinder = new LinkFinder(m_whelk.getStorage());
        if (parameters.getMergeRuleFile() != null) {
            Map rulesMap = mapper.readValue(Files.readString(parameters.getMergeRuleFile()), Map.class);
            m_merge = new Merge(rulesMap);
        }
        if (parameters.getChangedIn() != null)
            IMPORT_SYSTEM_CODE = parameters.getChangedIn();
        else
            IMPORT_SYSTEM_CODE = "batch import";
    }

    /**
     * Write a ISO2709 MarcRecord to LibrisXL. returns a resource ID if the resulting document (merged or new) was in "bib".
     * This ID should then be passed (as 'relatedWithBibResourceId') when importing any subsequent related holdings post.
     * Returns null when supplied a hold post.
     */
    String importISO2709(MarcRecord incomingMarcRecord,
                         String relatedWithBibResourceId,
                         Counter importedBibRecords,
                         Counter importedHoldRecords,
                         Counter encounteredMulBibs)
            throws Exception
    {
        String collection = "bib"; // assumption
        if (incomingMarcRecord.getLeader(6) == 'u' || incomingMarcRecord.getLeader(6) == 'v' ||
                incomingMarcRecord.getLeader(6) == 'x' || incomingMarcRecord.getLeader(6) == 'y')
            collection = "hold";

        if (collection.equals("hold") && relatedWithBibResourceId.equals(DISCARD_ATTACHED_HOLDINGS_MARKER))
        {
            // discard/ignore
            return null;
        }

        Set<String> duplicateIDs = getDuplicates(incomingMarcRecord, collection, relatedWithBibResourceId);

        String resultingResourceId = null;

        //System.err.println("Incoming [" + collection + "] document had: " + duplicateIDs.size() + " existing duplicates:\n" + duplicateIDs);

        // If an incoming holding record is marked deleted, attempt to find any duplicates for it in Libris and delete them.
        if (collection.equals("hold") && incomingMarcRecord.getLeader(5) == 'd')
        {
            for (String id : duplicateIDs)
                m_whelk.remove(id, IMPORT_SYSTEM_CODE, null);
            return null;
        }

        if (duplicateIDs.size() == 0) // No coinciding documents, simple import
        {
            if (collection.equals("bib") && m_parameters.getIgnoreNewBib())
            {
                // We matched nothing, and have been asked to not create new bib records. Return a signal that any
                // associated holdings must now be discarded.
                return DISCARD_ATTACHED_HOLDINGS_MARKER;
            }

            resultingResourceId = importNewRecord(incomingMarcRecord, collection, relatedWithBibResourceId, null);

            if (collection.equals("bib"))
                importedBibRecords.inc();
            else
                importedHoldRecords.inc();
        }
        else if (duplicateIDs.size() == 1) // merge, keep or replace
        {
            // replace
            if ((m_parameters.getReplaceBib() && collection.equals("bib")) ||
                    m_parameters.getReplaceHold() && collection.equals("hold"))
            {
                String idToReplace = duplicateIDs.iterator().next();
                resultingResourceId = importNewRecord(incomingMarcRecord, collection, relatedWithBibResourceId, idToReplace);
            }

            // merge
            else if (m_merge != null && collection.equals("bib")) {
                String idToMerge = duplicateIDs.iterator().next();
                Document incoming = convertToRDF(incomingMarcRecord, idToMerge);
                if (m_parameters.getReadOnly()) {
                    Document existing = m_whelk.getDocument(idToMerge);
                    History existingHistory = new History(m_whelk.getStorage().loadDocumentHistory(existing.getShortId()), m_whelk.getJsonld());
                    m_merge.merge(existing, incoming, m_parameters.getChangedBy(), existingHistory);
                    System.out.println("info: Would now (if --live had been specified) have written the following json-ld to whelk as a merged record:\n"
                            + existing.getDataAsString());
                }
                else {
                    m_whelk.storeAtomicUpdate(idToMerge, false, false, IMPORT_SYSTEM_CODE, m_parameters.getChangedBy(), (Document existing) -> {
                        String existingChecksum = existing.getChecksum(m_whelk.getJsonld());

                        List<String> recordIDs = existing.getRecordIdentifiers();
                        List<String> thingIDs = existing.getThingIdentifiers();
                        String controlNumber = existing.getControlNumber();
                        List<Tuple> typedIDs = existing.getTypedRecordIdentifiers();
                        List<String> systemNumbers = new ArrayList<>();
                        for (Tuple tuple : typedIDs)
                            if (tuple.get(0).equals("SystemNumber"))
                                systemNumbers.add( (String) tuple.get(1) );
                        List<Map<String, String>> images = existing.getImages();

                        History existingHistory = new History(m_whelk.getStorage().loadDocumentHistory(existing.getShortId()), m_whelk.getJsonld());
                        m_merge.merge(existing, incoming, m_parameters.getChangedBy(), existingHistory);

                        // The mainID must remain unaffected.
                        existing.deepPromoteId(recordIDs.get(0));

                        for (String recordID : recordIDs)
                            existing.addRecordIdentifier(recordID);
                        for (String thingID : thingIDs)
                            existing.addThingIdentifier(thingID);
                        for (String systemNumber : systemNumbers)
                            existing.addTypedRecordIdentifier("SystemNumber", systemNumber);
                        if (controlNumber != null)
                            existing.setControlNumber(controlNumber);
                        for (Map<String, String> imageEntity : images)
                            existing.addImage( imageEntity.get("@id") );

                        String modifiedChecksum = existing.getChecksum(m_whelk.getJsonld());
                        // Avoid writing an identical version
                        if (modifiedChecksum.equals(existingChecksum))
                            throw new CancelUpdateException();
                    });
                }

                resultingResourceId = m_whelk.getStorage().getThingMainIriBySystemId(idToMerge);
            }

            // Keep existing
            else
            {
                if (collection.equals("bib"))
                {
                    String duplicateId = (String) duplicateIDs.toArray()[0];
                    if (!duplicateId.startsWith(Document.getBASE_URI().toString()))
                        duplicateId = Document.getBASE_URI().toString() + duplicateId;
                    resultingResourceId = m_whelk.getStorage().getThingId(duplicateId);
                }
                else
                    resultingResourceId = null;
            }
        }
        else
        {
            // Multiple coinciding documents.
            encounteredMulBibs.inc();

            if (collection.equals("bib"))
            {
                // In order to keep the program deterministic, the bib post to which subsequent holdings should attach
                // when there are multiple duplicates is defined as the one with the "lowest" alpha numeric id.
                List<String> duplicateList = new ArrayList<>(duplicateIDs);
                Collections.sort(duplicateList);
                String selectedDuplicateId = duplicateList.get(0);

                logger.info("Incoming record:\n" + incomingMarcRecord.toString() + "was matched with more than one record: " + duplicateList + "." +
                        " Associated holdings will be added to: " + selectedDuplicateId);

                if (!selectedDuplicateId.startsWith(Document.getBASE_URI().toString()))
                    selectedDuplicateId = Document.getBASE_URI().toString() + selectedDuplicateId;
                resultingResourceId = m_whelk.getStorage().getThingId(selectedDuplicateId);
            }
            else
                resultingResourceId = null;
        }

        return resultingResourceId;
    }

    private String importNewRecord(MarcRecord marcRecord, String collection, String relatedWithBibResourceId, String replaceSystemId)
    {

        String incomingId = IdGenerator.generate();
        if (replaceSystemId != null)
            incomingId = replaceSystemId;

        Document rdfDoc = convertToRDF(marcRecord, incomingId);
        if (collection.equals("hold"))
        {
            String libraryUri = rdfDoc.getHeldBy();
            String librarySystemId = m_whelk.getStorage().getSystemIdByIri(libraryUri);
            if (librarySystemId == null)
            {
                logger.warn(String.format("Could not find %s. Not importing holding record for %s",
                        libraryUri, relatedWithBibResourceId));
                return null;
            }
            Document libraryDoc = m_whelk.getDocument(librarySystemId);
            if (!libraryDoc.libraryIsRegistrant())
            {
                logger.info(String.format("%s does not have category Registrant. Not importing holding record for %s",
                        libraryUri, relatedWithBibResourceId));
                return null;
            }
            rdfDoc.setHoldingFor(relatedWithBibResourceId);
        }

        String encodingLevel = rdfDoc.getEncodingLevel();
        if (encodingLevel == null || (
                !encodingLevel.equals(ENC_PRELIMINARY_STATUS) &&
                !encodingLevel.equals(ENC_PREPUBLICATION_STATUS) &&
                !encodingLevel.equals(ENC_ABBREVIVATED_STATUS) &&
                !encodingLevel.equals(ENC_MINMAL_STATUS)))
            rdfDoc.setRecordStatus(ENC_PRELIMINARY_STATUS);

        if (!m_parameters.getReadOnly())
        {
            // Doing a replace (but preserving old IDs)
            if (replaceSystemId != null)
            {
                try
                {
                    m_whelk.storeAtomicUpdate(replaceSystemId, false, false, IMPORT_SYSTEM_CODE, m_parameters.getChangedBy(),
                            (Document doc) ->
                    {
                        String existingEncodingLevel = doc.getEncodingLevel();
                        String newEncodingLevel = rdfDoc.getEncodingLevel();

                        if (!collection.equals("hold"))
                            if (existingEncodingLevel == null || !mayOverwriteExistingEncodingLevel(existingEncodingLevel, newEncodingLevel))
                                throw new TooHighEncodingLevelException();

                        List<String> recordIDs = doc.getRecordIdentifiers();
                        List<String> thingIDs = doc.getThingIdentifiers();
                        String controlNumber = doc.getControlNumber();
                        List<Tuple> typedIDs = doc.getTypedRecordIdentifiers();
                        List<String> systemNumbers = new ArrayList<>();
                        for (Tuple tuple : typedIDs)
                            if (tuple.get(0).equals("SystemNumber"))
                                systemNumbers.add( (String) tuple.get(1) );
                        List<Map<String, String>> images = doc.getImages();

                        doc.data = rdfDoc.data;

                        // The mainID must remain unaffected.
                        doc.deepPromoteId(recordIDs.get(0));

                        for (String recordID : recordIDs)
                            doc.addRecordIdentifier(recordID);
                        for (String thingID : thingIDs)
                            doc.addThingIdentifier(thingID);
                        for (String systemNumber : systemNumbers)
                            doc.addTypedRecordIdentifier("SystemNumber", systemNumber);
                        if (controlNumber != null)
                            doc.setControlNumber(controlNumber);
                        for (Map<String, String> imageEntity : images)
                            doc.addImage( imageEntity.get("@id") );
                    });
                }
                catch (TooHighEncodingLevelException e)
                {
                    if ( verbose )
                    {
                        System.out.println("info: Not replacing id: " + replaceSystemId + ", because it no longer has encoding level marc:PartialPreliminaryLevel");
                    }
                }
            }
            else
            {
                // Doing simple "new"
                m_whelk.createDocument(rdfDoc, IMPORT_SYSTEM_CODE, m_parameters.getChangedBy(), collection, false);
            }
        }
        else
        {
            System.out.println("info: Would now (if --live had been specified) have written the following json-ld to whelk as a new record:\n"
                    + rdfDoc.getDataAsString());
        }

        if (collection.equals("bib"))
            return rdfDoc.getThingIdentifiers().get(0);
        return null;
    }

    private boolean mayOverwriteExistingEncodingLevel(String existingEncodingLevel, String newEncodingLevel)
    {
        if (m_parameters.getForceUpdate())
            return true;

        if (newEncodingLevel == null || existingEncodingLevel == null)
            return false;

        Set<String> specialRule = m_parameters.getSpecialRules().get(newEncodingLevel);

        if (specialRule != null && specialRule.contains(existingEncodingLevel))
            return true;

        switch (newEncodingLevel)
        {
            case ENC_PRELIMINARY_STATUS: // 5
                if (existingEncodingLevel.equals(ENC_PRELIMINARY_STATUS)) // 5
                    return true;
                break;
            case ENC_PREPUBLICATION_STATUS: // 8
                if (existingEncodingLevel.equals(ENC_PRELIMINARY_STATUS) || existingEncodingLevel.equals(ENC_PREPUBLICATION_STATUS)) // 5 || 8
                    return true;
                break;
            case ENC_ABBREVIVATED_STATUS: // 3
                if (existingEncodingLevel.equals(ENC_PRELIMINARY_STATUS) || existingEncodingLevel.equals(ENC_PREPUBLICATION_STATUS)) // 5 || 8
                    return true;
                break;
            case ENC_MINMAL_STATUS: // 7
                if (existingEncodingLevel.equals(ENC_PRELIMINARY_STATUS) || existingEncodingLevel.equals(ENC_PREPUBLICATION_STATUS)) // 5 || 8
                    return true;
                break;
        }
        return false;
    }

    private Document convertToRDF(MarcRecord _marcRecord, String id)
    {
        MarcRecord marcRecord = cloneMarcRecord(_marcRecord);
        LegacyIntegrationTools.makeRecordLibrisResident(marcRecord);
        while (marcRecord.getControlfields("001").size() > 0)
            marcRecord.getFields().remove(marcRecord.getControlfields("001").get(0));
        marcRecord.addField(marcRecord.createControlfield("001", id));

        // Filter out 887 fields, as the converter cannot/should not handle them
        Iterator<Field> it = marcRecord.getFields().iterator();
        while (it.hasNext()){
            Field field = it.next();
            if (field.getTag().equals("887"))
                it.remove();
        }

        Map convertedData = m_marcFrameConverter.convert(MarcJSONConverter.toJSONMap(marcRecord), id);
        Document convertedDocument = new Document(convertedData);
        convertedDocument.deepReplaceId(Document.getBASE_URI().toString()+id);
        m_linkfinder.normalizeIdentifiers(convertedDocument);
        m_whelk.getNormalizer().normalize(convertedDocument);
        return convertedDocument;
    }

    private MarcRecord cloneMarcRecord(MarcRecord original)
    {
        MarcRecord clone = new MarcRecordImpl();
        for (Field f : original.getFields())
            clone.addField(f);
        clone.setLeader(original.getLeader());
        for (Object key : original.getProperties().keySet())
            clone.setProperty( (String) key, original.getProperties().get(key));
        clone.setOriginalData(original.getOriginalData());

        return clone;
    }

    private Set<String> getDuplicates(MarcRecord marcRecord, String collection, String relatedWithBibResourceId)
            throws SQLException, IsbnException
    {
        switch (collection)
        {
            case "bib":
                return getBibDuplicates(marcRecord);
            case "hold":
                return getHoldDuplicates(marcRecord, relatedWithBibResourceId);
            default:
                return new HashSet<>();
        }
    }

    private Set<String> getHoldDuplicates(MarcRecord marcRecord, String relatedWithBibResourceId)
            throws SQLException
    {
        Set<String> duplicateIDs = new HashSet<>();

        // Assumes the post being imported carries a valid libris id in 001, and "SE-LIBR" or "LIBRIS" in 003
        duplicateIDs.addAll(getDuplicatesOnLibrisID(marcRecord, "hold"));
        duplicateIDs.addAll(getDuplicatesOnHeldByHoldingFor(marcRecord, relatedWithBibResourceId));

        return duplicateIDs;
    }

    private Set<String> getBibDuplicates(MarcRecord marcRecord)
            throws SQLException, IsbnException
    {
        Set<String> duplicateIDs = new HashSet<>();

        // Perform an temporary conversion to use for duplicate checking. This conversion will
        // then be discarded. The real conversion cannot take place until any duplicates are
        // found (because the correct ID needs to be known when converting). Chicken and egg problem.
        Document rdfDoc = convertToRDF(marcRecord, IdGenerator.generate());

        for (Parameters.DUPLICATION_TYPE dupType : m_parameters.getDuplicationTypes())
        {
            switch (dupType)
            {
                case DUPTYPE_ISBNA: // International Standard Book Number (only from subfield A)
                    for (String isbn : rdfDoc.getIsbnValues())
                    {
                        duplicateIDs.addAll(getDuplicatesOnIsbn( isbn.toUpperCase(), this::getOnIsbn_ps ));
                    }
                    break;
                case DUPTYPE_ISBNZ: // International Standard Book Number (only from subfield Z)
                    for (String isbn : rdfDoc.getIsbnHiddenValues())
                    {
                        duplicateIDs.addAll(getDuplicatesOnIsbn( isbn.toUpperCase(), this::getOnIsbnHidden_ps ));
                    }
                    break;
                case DUPTYPE_ISSNA: // International Standard Serial Number (only from marc 022_A)
                    for (String issn : rdfDoc.getIssnValues())
                    {
                            duplicateIDs.addAll(getDuplicatesOnIssn( issn.toUpperCase() ));
                    }
                    break;
                case DUPTYPE_ISSNZ: // International Standard Serial Number (only from marc 022_Z)
                    for (String issn : rdfDoc.getIssnHiddenValues())
                    {
                        duplicateIDs.addAll(getDuplicatesOnIssnHidden( issn.toUpperCase() ));
                    }
                    break;
                case DUPTYPE_035A:
                    // Unique id number in another system.
                    duplicateIDs.addAll(getDuplicatesOn035a(marcRecord));
                    break;
                case DUPTYPE_LIBRISID:
                    // Assumes the post being imported carries a valid libris id in 001, and "SE-LIBR" or "LIBRIS" in 003
                    duplicateIDs.addAll(getDuplicatesOnLibrisID(marcRecord, "bib"));
                    break;
                case DUPTYPE_EAN:
                    // Unique id number in another system.
                    duplicateIDs.addAll(getDuplicatesOnEAN(marcRecord));
                    break;
                case DUPTYPE_URI:
                    // Unique id number in another system.
                    duplicateIDs.addAll(getDuplicatesOnURI(marcRecord));
                    break;
                case DUPTYPE_URN:
                    // Unique id number in another system.
                    duplicateIDs.addAll(getDuplicatesOnURN(marcRecord));
                    break;
            }

            // If duplicates have already been found, do not try any more duplicate types.
            if (!duplicateIDs.isEmpty())
                break;
        }

        // Filter duplicate list on compatible instance types.
        Iterator<String> it = duplicateIDs.iterator();
        Set<String> disqualifiedIDs = new HashSet<>();
        while (it.hasNext()) {
            String candidateID = it.next();
            Document candidate = m_whelk.getStorage().load(candidateID);

            String incomingInstanceType = rdfDoc.getThingType();
            String existingInstanceType = candidate.getThingType();

            if (!existingInstanceType.equals(incomingInstanceType)
                    && !existingInstanceType.equals("Instance")
                    && !incomingInstanceType.equals("Instance")) {
                disqualifiedIDs.add(candidateID);
                it.remove();
            }
        }
        if (!disqualifiedIDs.isEmpty() && duplicateIDs.isEmpty()) {
            logger.info("An incoming record was matched with: " + disqualifiedIDs + ", all of which were disqualified due to mismatching main entity types. " +
                    "A new record with the correct main entity type will be created instead.");
        }

        return duplicateIDs;
    }

    private List<String> getDuplicatesOnLibrisID(MarcRecord marcRecord, String collection)
            throws SQLException
    {
        String librisId = DigId.grepLibrisId(marcRecord);

        if (librisId == null)
            return new ArrayList<>();

        // completely numeric? = classic voyager id.
        // In theory an xl id could (though insanely unlikely) also be numeric :(
        if (librisId.matches("[0-9]+"))
        {
            librisId = "http://libris.kb.se/"+collection+"/"+librisId;
        }
        else if ( ! librisId.startsWith(Document.getBASE_URI().toString()))
        {
            librisId = Document.getBASE_URI().toString() + librisId;
        }

        try(PostgreSQLComponent.ConnectionContext ignored =
                    new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
            PreparedStatement statement = getOnId_ps(m_whelk.getStorage().getMyConnection(), librisId);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private List<String> getDuplicatesOn035a(MarcRecord marcRecord)
            throws SQLException
    {
        List<String> results = new ArrayList<>();
        for (Field field : marcRecord.getFields("035"))
        {
            String systemNumber = DigId.grep035a( (Datafield) field );

            try(PostgreSQLComponent.ConnectionContext ignored =
                        new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
                PreparedStatement statement = getOnSystemNumber_ps(m_whelk.getStorage().getMyConnection(), systemNumber);
                ResultSet resultSet = statement.executeQuery())
            {
                results.addAll( collectIDs(resultSet) );
            }
        }
        return results;
    }

    private List<String> getDuplicatesOnEAN(MarcRecord marcRecord)
            throws SQLException
    {
        List<String> results = new ArrayList<>();
        for (Datafield field : marcRecord.getDatafields("024"))
        {
            if (field.getIndicator(0) == '3')
            {
                List<Subfield> subfields = field.getSubfields("a");
                for (Subfield subfield : subfields)
                {
                    String incomingEan = subfield.getData();

                    try (PostgreSQLComponent.ConnectionContext ignored =
                                 new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
                         PreparedStatement statement = getOnEAN_ps(m_whelk.getStorage().getMyConnection(), incomingEan);
                         ResultSet resultSet = statement.executeQuery())
                    {
                        results.addAll(collectIDs(resultSet));
                    }
                }
            }
        }
        return results;
    }

    private List<String> getDuplicatesOnURI(MarcRecord marcRecord)
            throws SQLException
    {
        List<String> results = new ArrayList<>();
        for (Datafield field : marcRecord.getDatafields("024"))
        {
            if (field.getIndicator(0) == '7')
            {
                List<Subfield> sf2uri = field.getSubfields("2").stream().filter(sf -> sf.getData().equalsIgnoreCase("uri")).collect(Collectors.toList());
		if ( sf2uri.size() == 0 ) continue;

                List<Subfield> subfields = field.getSubfields("a");
                for (Subfield subfield : subfields)
                {
                    String incomingUri = subfield.getData();

                    try (PostgreSQLComponent.ConnectionContext ignored =
                                 new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
                         PreparedStatement statement = getOnURI_ps(m_whelk.getStorage().getMyConnection(), incomingUri);
                         ResultSet resultSet = statement.executeQuery())
                    {
                        results.addAll(collectIDs(resultSet));
                    }
                }
            }
        }
        return results;
    }

    private List<String> getDuplicatesOnURN(MarcRecord marcRecord)
            throws SQLException
    {
        List<String> results = new ArrayList<>();
        for (Datafield field : marcRecord.getDatafields("024"))
        {
            if (field.getIndicator(0) == '7')
            {
                List<Subfield> sf2urn = field.getSubfields("2").stream().filter(sf -> sf.getData().equalsIgnoreCase("urn")).collect(Collectors.toList());
		if ( sf2urn.size() == 0 ) continue;

                List<Subfield> subfields = field.getSubfields("a");
                for (Subfield subfield : subfields)
                {
                    String incomingUrn = subfield.getData();

                    try (PostgreSQLComponent.ConnectionContext ignored =
                                 new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
                         PreparedStatement statement = getOnURN_ps(m_whelk.getStorage().getMyConnection(), incomingUrn);
                         ResultSet resultSet = statement.executeQuery())
                    {
                        results.addAll(collectIDs(resultSet));
                    }
                }
            }
        }
        return results;
    }

    private List<String> getDuplicatesOnIsbn(String isbn, BiFunction<Connection, String, PreparedStatement> getPreparedStatement)
            throws SQLException, IsbnException
    {
        boolean hyphens = false;
        if (isbn == null || isbn.length() == 0)
            return new ArrayList<>();

        List<String> duplicateIDs = new ArrayList<>();

        try(PostgreSQLComponent.ConnectionContext ignored =
                    new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
            PreparedStatement statement = getPreparedStatement.apply(m_whelk.getStorage().getMyConnection(), isbn);
            ResultSet resultSet = statement.executeQuery())
        {
            duplicateIDs.addAll( collectIDs(resultSet) );
        }

        Isbn typedIsbn;
        try {
            typedIsbn = IsbnParser.parse(isbn);
        } catch (Exception e) {
            // Bad ISBN values can occur, we should ignore them, but not crash.
            return duplicateIDs;
        }
        if (typedIsbn == null)
            return duplicateIDs;

        int otherType = typedIsbn.getType() == Isbn.ISBN10 ? Isbn.ISBN13 : Isbn.ISBN10;

        String numericIsbn = typedIsbn.toString(hyphens);
        try(PostgreSQLComponent.ConnectionContext ignored =
                    new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
            PreparedStatement statement = getPreparedStatement.apply(m_whelk.getStorage().getMyConnection(), numericIsbn);
            ResultSet resultSet = statement.executeQuery())
        {
            duplicateIDs.addAll( collectIDs(resultSet) );
        }

        // Collect additional duplicates with the other ISBN form (if conversion is possible)
        try
        {
            typedIsbn = typedIsbn.convert(otherType);
        } catch (ConvertException ce)
        {
            return duplicateIDs;
        }
        numericIsbn = typedIsbn.toString(hyphens);
        try(PostgreSQLComponent.ConnectionContext ignored =
                    new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
            PreparedStatement statement = getPreparedStatement.apply(m_whelk.getStorage().getMyConnection(), numericIsbn);
            ResultSet resultSet = statement.executeQuery())
        {
            duplicateIDs.addAll( collectIDs(resultSet) );
        }

        return duplicateIDs;
    }

    private List<String> getDuplicatesOnIssn(String issn)
            throws SQLException
    {
        if (issn == null)
            return new ArrayList<>();

        try(PostgreSQLComponent.ConnectionContext ignored =
                    new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
            PreparedStatement statement = getOnIssn_ps(m_whelk.getStorage().getMyConnection(), issn);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private List<String> getDuplicatesOnIssnHidden(String issn)
            throws SQLException
    {
        if (issn == null)
            return new ArrayList<>();

        try(PostgreSQLComponent.ConnectionContext ignored =
                    new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
            PreparedStatement statement = getOnIssnHidden_ps(m_whelk.getStorage().getMyConnection(), issn);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private List<String> getDuplicatesOnHeldByHoldingFor(MarcRecord marcRecord, String relatedWithBibResourceId)
            throws SQLException
    {
        if (marcRecord.getFields("852").size() < 1)
            return new ArrayList<>();
        Datafield df = (Datafield) marcRecord.getFields("852").get(0);
        if (df.getSubfields("b").size() < 1)
            return new ArrayList<>();
        String sigel = df.getSubfields("b").get(0).getData();
        String library = LegacyIntegrationTools.legacySigelToUri(sigel);

        try(PostgreSQLComponent.ConnectionContext ignored =
                    new PostgreSQLComponent.ConnectionContext(m_whelk.getStorage().connectionContextTL);
            PreparedStatement statement = getOnHeldByHoldingFor_ps(m_whelk.getStorage().getMyConnection(), library, relatedWithBibResourceId);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private PreparedStatement getOnId_ps(Connection connection, String id)
            throws SQLException
    {
        String query = "SELECT lddb__identifiers.id FROM lddb__identifiers JOIN lddb ON lddb__identifiers.id = lddb.id WHERE lddb__identifiers.iri = ? AND lddb.deleted = false";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, id);
        return statement;
    }

    /**
     * "System number" is our ld equivalent of marc's 035a
     */
    private PreparedStatement getOnSystemNumber_ps(Connection connection, String systemNumber)
            throws SQLException
    {
        String query = "SELECT id FROM lddb WHERE deleted = false AND data#>'{@graph,0,identifiedBy}' @> ?";
        PreparedStatement statement =  connection.prepareStatement(query);

        statement.setObject(1, "[{\"@type\": \"SystemNumber\", \"value\": \"" + StringEscapeUtils.escapeJavaScript(systemNumber) + "\"}]", java.sql.Types.OTHER);

        return statement;
    }

    private PreparedStatement getOnIsbn_ps(Connection connection, String isbn)
    {
        try
        {
            String query = "SELECT id FROM lddb WHERE collection = 'bib' AND deleted = false AND data#>'{@graph,1,identifiedBy}' @> ?";
            PreparedStatement statement = connection.prepareStatement(query);

            statement.setObject(1, "[{\"@type\": \"ISBN\", \"value\": \"" + StringEscapeUtils.escapeJavaScript(isbn) + "\"}]", java.sql.Types.OTHER);

            return statement;
        } catch (SQLException se)
        {
            throw new RuntimeException(se);
        }
    }

    private PreparedStatement getOnIssn_ps(Connection connection, String issn)
            throws SQLException
    {
        String query = "SELECT id FROM lddb WHERE collection = 'bib' AND deleted = false AND data#>'{@graph,1,identifiedBy}' @> ?";
        PreparedStatement statement =  connection.prepareStatement(query);

        statement.setObject(1, "[{\"@type\": \"ISSN\", \"value\": \"" + StringEscapeUtils.escapeJavaScript(issn) + "\"}]", java.sql.Types.OTHER);

        return statement;
    }

    private PreparedStatement getOnIsbnHidden_ps(Connection connection, String isbn)
    {
        try
        {
            String query = "SELECT id FROM lddb WHERE collection = 'bib' AND deleted = false AND data#>'{@graph,1,indirectlyIdentifiedBy}' @> ?";
            PreparedStatement statement = connection.prepareStatement(query);

            statement.setObject(1, "[{\"@type\": \"ISBN\", \"value\": \"" + StringEscapeUtils.escapeJavaScript(isbn) + "\"}]", java.sql.Types.OTHER);

            return statement;
        } catch (SQLException se)
        {
            throw new RuntimeException(se);
        }
    }

    private PreparedStatement getOnIssnHidden_ps(Connection connection, String issn)
            throws SQLException
    {
        String query = "SELECT id FROM lddb WHERE collection = 'bib' AND deleted = false AND ( data#>'{@graph,1,identifiedBy}' @> ? OR data#>'{@graph,1,identifiedBy}' @> ?)";
        PreparedStatement statement =  connection.prepareStatement(query);

        statement.setObject(1, "[{\"@type\": \"ISSN\", \"marc:canceledIssn\": [\"" + StringEscapeUtils.escapeJavaScript(issn) + "\"]}]", java.sql.Types.OTHER);
        statement.setObject(2, "[{\"@type\": \"ISSN\", \"marc:canceledIssn\": \"" + StringEscapeUtils.escapeJavaScript(issn) + "\"}]", java.sql.Types.OTHER);

        return statement;
    }

    private PreparedStatement getOnEAN_ps(Connection connection, String ean)
            throws SQLException
    {
        String query = "SELECT id FROM lddb WHERE collection = 'bib' AND deleted = false AND data#>'{@graph,1,identifiedBy}' @> ?";
        PreparedStatement statement =  connection.prepareStatement(query);

        statement.setObject(1, "[{\"@type\": \"EAN\", \"value\": \"" + StringEscapeUtils.escapeJavaScript(ean) + "\"}]", java.sql.Types.OTHER);

        return statement;
    }

    private PreparedStatement getOnURI_ps(Connection connection, String uri)
            throws SQLException
    {
        String query = "SELECT id FROM lddb WHERE collection = 'bib' AND deleted = false AND data#>'{@graph,1,identifiedBy}' @> ?";
        PreparedStatement statement =  connection.prepareStatement(query);

        statement.setObject(1, "[{\"@type\": \"URI\", \"value\": \"" + StringEscapeUtils.escapeJavaScript(uri) + "\"}]", java.sql.Types.OTHER);

        return statement;
    }

    private PreparedStatement getOnURN_ps(Connection connection, String urn)
            throws SQLException
    {
        String query = "SELECT id FROM lddb WHERE collection = 'bib' AND deleted = false AND data#>'{@graph,1,identifiedBy}' @> ?";
        PreparedStatement statement =  connection.prepareStatement(query);

        statement.setObject(1, "[{\"@type\": \"URN\", \"value\": \"" + StringEscapeUtils.escapeJavaScript(urn) + "\"}]", java.sql.Types.OTHER);

        return statement;
    }

    private PreparedStatement getOnHeldByHoldingFor_ps(Connection connection, String heldBy, String holdingForId)
            throws SQLException
    {
        String libraryUri = LegacyIntegrationTools.legacySigelToUri(heldBy);

        // Here be dragons. The always-works query is this:
        /*String query =
                "SELECT lddb.id from lddb " +
                "INNER JOIN lddb__identifiers id1 ON lddb.data#>>'{@graph,1,itemOf,@id}' = id1.iri " +
                "INNER JOIN lddb__identifiers id2 ON id1.id = id2.id " +
                "WHERE " +
                "data#>>'{@graph,1,heldBy,@id}' = ? " +
                "AND " +
                "id2.iri = ?";*/

        // This query REQUIRES that links be on the primary ID only. This works beacuse of link-finding step2, but if
        // that should ever change this query would break.

        String query = "SELECT id from lddb WHERE deleted = false AND data#>>'{@graph,1,heldBy,@id}' = ? AND data#>>'{@graph,1,itemOf,@id}' = ? AND deleted = false";

        PreparedStatement statement = connection.prepareStatement(query);

        statement.setString(1, libraryUri);
        statement.setString(2, holdingForId);

        return statement;
    }

    private List<String> collectIDs(ResultSet resultSet)
            throws SQLException
    {
        List<String> ids = new ArrayList<>();
        while (resultSet.next())
        {
            ids.add(resultSet.getString("id"));
        }
        return ids;
    }

}

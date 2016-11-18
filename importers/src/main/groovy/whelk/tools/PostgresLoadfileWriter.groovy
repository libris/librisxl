package whelk.tools

import groovy.json.JsonBuilder
import groovy.sql.Sql
import groovy.util.logging.Slf4j as Log
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.Document
import whelk.converter.MarcJSONConverter
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.MySQLLoader
import whelk.util.LegacyIntegrationTools

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.ResultSet
import java.sql.Statement

//import static groovyx.gpars.dataflow.Dataflow.task
//import static groovyx.gpars.GParsPool.withPool
/**
 * Writes documents into a PostgreSQL load-file, which can be efficiently imported into lddb
 */
@Log
class PostgresLoadfileWriter {
    private static
    final int THREAD_COUNT = Runtime.runtime.availableProcessors();
    private static final int CONVERSIONS_PER_THREAD = 100;

    // USED FOR DEV ONLY, MUST _NEVER_ BE SET TO TRUE ONCE XL GOES INTO PRODUCTION. WITH THIS SETTING THE IMPORT WILL
    // _SKIP_ DOCUMENTS THAT FAIL CONVERSION, RESULTING IN POTENTIAL DATA LOSS IF USED WHEN IMPORTING TO A PRODUCTION XL
    private static final boolean FAULT_TOLERANT_MODE = false;

    private static MarcFrameConverter s_marcFrameConverter;
    private static BufferedWriter s_mainTableWriter;
    private static BufferedWriter s_identifiersWriter;
    private static Thread[] s_threadPool;
    private static Vector<String> s_failedIds = new Vector<String>();

    // Abort on unhandled exceptions, including those on worker threads.
    static
    {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            void uncaughtException(Thread thread, Throwable throwable) {
                System.out.println("PANIC ABORT, unhandled exception:\n");
                throwable.printStackTrace();
                System.exit(-1);
            }
        });
    }

    public
    static void dumpGpars(String exportFileName, String collection, String connectionUrl) {
        if (FAULT_TOLERANT_MODE)
            System.out.println("\t**** RUNNING IN FAULT TOLERANT MODE, DOCUMENTS THAT FAIL CONVERSION WILL BE SKIPPED.\n" +
                    "\tIF YOU ARE IMPORTING TO A PRODUCTION XL, ABORT NOW!! AND RECOMPILE WITH FAULT_TOLERANT_MODE=false");

        s_marcFrameConverter = new MarcFrameConverter();
        s_mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"));
        s_identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"));
        def counter = 0
        def successfulMatches = 0
        def totallMatches
        def startTime = System.currentTimeMillis()
        //withPool {
        try {
            def sql = Sql.newInstance(connectionUrl, "com.mysql.jdbc.Driver")
            sql.withStatement { Statement stmt -> stmt.fetchSize = Integer.MIN_VALUE }
            sql.connection.autoCommit = false
            sql.resultSetType = ResultSet.TYPE_FORWARD_ONLY
            sql.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY

            List<Map> previousAuthData = []
            Map previousBibResultSet = null

            sql.eachRow(MySQLLoader.selectByMarcType[collection], [0]) { ResultSet currentRow ->
                try {
                    Map rowMap = [data      : currentRow.getBytes('data'),
                                  created   : currentRow.getTimestamp('create_date'),
                                  collection: collection]

                    if (++counter % 10000 == 0) {
                        def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                        if (elapsedSecs > 0) {
                            def docsPerSec = counter / elapsedSecs
                            println "Working. Currently ${counter} documents saved. Crunching ${docsPerSec} docs / s"
                            println "Matches: ${successfulMatches}/${totallMatches}"
                        }
                    }


                    switch (collection) {
                        case 'auth':
                            handleRow(rowMap, collection, null)
                            break
                        case 'hold':
                            rowMap.put('sigel', currentRow.getString("shortname"))
                            handleRow(rowMap, collection, getHoldOaipmhSetSpecs(rowMap))
                            break
                        case 'bib':
                            int currentRecordId = currentRow.getInt('bib_id')
                            rowMap.put('bib_id', currentRecordId)
                            rowMap.put('auth_id', currentRow.getInt('auth_id'))
                            Map currentAuthData = rowMap.auth_id > 0 ?
                                    [bibid: rowMap.bib_id,
                                     id   : rowMap.auth_id,
                                     data : getMarcDocMap(currentRow.getBytes('auth_data'))] : null
                            if (rowMap.auth_id > 0 && currentAuthData.data.fields != null) {
                                currentAuthData << composeAuthData(currentAuthData)
                            }

                            switch (previousBibResultSet) {
                            //first run
                                case null:
                                    previousBibResultSet = rowMap
                                    previousAuthData.add(currentAuthData)
                                    break
                            //Same bib record
                                case { it.bib_id == currentRecordId }:
                                    //print "."
                                    previousAuthData.add(currentAuthData)
                                    break
                            //New record
                                default:
                                    //print "| "
                                    // task {
                                    handleRow(previousBibResultSet, collection, previousAuthData)
                                    //}
                                    previousBibResultSet = rowMap
                                    previousAuthData = []
                                    previousAuthData.add(currentAuthData)
                            }
                            break
                    }
                }
                catch (any) {
                    print any.message
                    print any.stackTrace
                    throw any
                }
            }

            //Last row
            handleRow(previousBibResultSet, collection, previousAuthData)

        }

        catch (any) {
            println any.message
            throw any
        }
        finally {
            s_mainTableWriter.close()
            s_identifiersWriter.close()
        }
        //}

        def endSecs = (System.currentTimeMillis() - startTime) / 1000
        println "Done. Processed  ${counter} documents in ${endSecs} seconds."

    }

    static def normaliseSubfields(def subfields) {
        subfields.collect {
            it.collect { k, v -> [(k): (v as String).replaceAll(/(^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+\u0024)|(\s)/, "").toLowerCase()] }[0]
        }

    }

    static Map composeAuthData(LinkedHashMap<String, Object> map) {
        //TODO: how common is it with repeated subfields, ie "a"?
        for (Map bibField in map.data.fields) {
            String key = bibField.keySet()[0]
            if (key.startsWith('1')) {
                return [field: key, subfields: bibField[key].subfields]
            }

        }

        def builder = new JsonBuilder(map)
        throw new Exception("Unhandled authority record ${builder.toPrettyString()} q")

        /*    for (Map bibField in map.data.fields) {
            String key = bibField.keySet()[0]
            switch (key) {
                case { fieldKey -> ['100'].contains(fieldKey) }:
                    //println "100"
                    return [field : '100',
                            author: map.data.fields."100".subfields.a?.first()?.find {
                                it
                            } ?: '',
                            date  : map.data.fields."100".subfields.d?.first()?.find {
                                it
                            } ?: '',
                            title : map.data.fields."100".subfields.t?.first()?.find {
                                it
                            } ?: '']
                case { fieldKey -> ['110'].contains(fieldKey) }:
                    //println "110"
                    return [
                            field: '110',
                            a    : map.data.fields."110".subfields.a?.first()?.first() ?: '',
                            b    : map.data.fields."110".subfields.a?.first()?.first() ?: ''

                    ]
                case { fieldKey -> ['130'].contains(fieldKey) }:
                    //println "130"
                    //TODO: Match on more fields?
                    return [
                            field: '130',
                            a    : map.data.fields."130".subfields.a?.first()?.first() ?: '',


                    ]
                case { fieldKey -> ['150'].contains(fieldKey) }:
                    //println "150"
                    return [
                            field: '150',
                            term : map.data.fields."150".subfields.a?.first()?.first() ?: '']
                case { fieldKey -> ['151'].contains(fieldKey) }:
                    //println "151"
                    //TODO: vilka delfält? https://www.loc.gov/marc/authority/ad151.html
                    return [
                            field: '151',
                            term : map.data.fields."151".subfields.a?.first()?.first() ?: '']
                case { fieldKey -> ['155'].contains(fieldKey) }:
                    //println "155"
                    //TODO: vilka delfält? https://www.loc.gov/marc/authority/ad151.html
                    return [
                            field: '155',
                            term : map.data.fields."155".subfields.a?.first()?.first() ?: '']
            //default:
            //println "skipped field ${key}"


            }
        }
        def builder = new JsonBuilder(map)
        throw new Exception("Unhandled authority record ${builder.toPrettyString()}")*/
    }

    static Map getMarcDocMap(byte[] data) {
        byte[] dataBytes = MySQLLoader.normalizeString(
                new String(data as byte[], "UTF-8"))
                .getBytes("UTF-8")

        MarcRecord record = Iso2709Deserializer.deserialize(dataBytes)

        if (record) {
            return MarcJSONConverter.toJSONMap(record)
        } else {
            return null
        }
    }

    private
    static void handleRow(Map rowMap, String collection, List setSpecs) {

        Map doc = getMarcDocMap(rowMap.data as byte[])
        List auhtLinkableFieldNames = ['100', '600', '700', '800', '110', '610', '710', '810', '130', '630', '730', '830', '650', '651', '655']
        List ignoredAuthFields = ['180', '181', '182', '185', '162']
        //TODO: franska diakriter

        Map fieldRules = [
                '100': [subFieldsToIgnore:
                                [bib : ['0', '4'],
                                 auth: ['6']],
                        bibFields        : ['100', '600', '700', '800']],
                '110': [subFieldsToIgnore:
                                [bib : ['0', '4'],
                                 auth: ['6']],
                        bibFields        : ['110', '610', '710']],
                '111': [subFieldsToIgnore:
                                [bib : ['0', '4'],
                                 auth: ['6']],
                        bibFields        : ['111', '611', '711']],
                '130': [subFieldsToIgnore:
                                [bib : ['0', '4'],
                                 auth: ['6']],
                        bibFields        : ['130', '630', '730', '830']],
                '150': [subFieldsToIgnore:
                                [bib : ['0', '2', '4'],
                                 auth: ['6']],
                        bibFields        : ['650']],
                '151': [subFieldsToIgnore:
                                [bib : ['0', '2', '4'],
                                 auth: ['6']],
                        bibFields        : ['651']],
                '155': [subFieldsToIgnore:
                                [bib : ['0', '2', '4'],
                                 auth: ['6']],
                        bibFields        : ['655']]
        ]
        if (doc) {

            if (collection == 'bib' && setSpecs.count { it } > 0) {

                List matchedFields = []
                List misMatchedFields = []
                //Iterate over the auth records. All should match
                setSpecs.findAll { it -> !ignoredAuthFields.contains(it.field) }.each { authField ->
                    def fieldRule = fieldRules[authField.field]
                    if (!fieldRule)
                        throw new Exception("Missing rule for authority field ${authField.field} bibid: ${authField.bibid}  auth: ${authField.id}")
                    def bibFields = doc.fields.findAll { bibField ->
                        fieldRule.bibFields.contains(bibField.keySet()[0])
                    }
                    def authSubFields = normaliseSubfields(authField.subfields)
                            .findAll { it -> !fieldRule.subFieldsToIgnore.auth.contains(it.keySet()[0]) }
                            .collect { it -> it }

                    bibFields.each { bibField ->
                        //def allSub = bibField[bibField.keySet()[0]].subfields
                        //if (allSub.any { it -> it.keySet()[0] == '0' }) {
                            def bibSubFields = normaliseSubfields(bibField[bibField.keySet()[0]].subfields)
                                    .findAll { it -> !fieldRule.subFieldsToIgnore.bib.contains(it.keySet()[0]) }
                                    .collect { it -> it }

                            def diff = bibSubFields.toSet() - authSubFields.toSet()
                            def diffKeys = diff.collect { it -> it.keySet().first() }
                            if (diff.count { it } > 0) {

                                misMatchedFields.add([bib: bibField.keySet()[0], authField: authField.field, diff: diff, auth: authSubFields, bib: bibSubFields])
                            }

                            if (diff.count { it } > 0 &&
                                    !diffKeys.contains('a') &&
                                    !(diffKeys.contains('b') && authField.field == '110') &&
                                    !(diffKeys.contains('p') && authField.field == '130') &&
                                    !(diffKeys.contains('z') && authField.field == '151') &&
                                    !(diffKeys.contains('x') && authField.field == '150') &&
                                    !(diffKeys.contains('y') && authField.field == '150') &&
                                    !(diffKeys.contains('t') && authField.field == '100') &&
                                    !(diffKeys.contains('0') && authField.field == '100')
                            )
                                println "Diff! Field: ${authField.field}/${bibField.keySet()[0]} ${diff} auth: ${authSubFields} bib: ${bibSubFields} bibid: https://libris.kb.se/bib/${authField.bibid} authid:https://libris.kb.se/auth/${authField.id}"

                            if (diff.count { it } == 0) {
                                authField.put('matched', 'matched')
                                matchedFields.add([diff: diff, bib: bibField.keySet()[0], auth: authField.field])
                                //bibField[bibField.keySet()[0]].subfields.add([0: "https://libris.kb.se/auth/${authField.id}"])
                            }
                        }
                    //}

                }
                if (matchedFields.count {it} > setSpecs.count { it -> !ignoredAuthFields.contains(it.field) }) {
                    println "Matches: ${matchedFields.count { it }}/${setSpecs.count { it -> !ignoredAuthFields.contains(it.field) }}"
                }
                if (matchedFields.count {it} < setSpecs.count { it -> !ignoredAuthFields.contains(it.field) }) {
                    println "Matches: ${matchedFields.count { it }}/${setSpecs.count { it -> !ignoredAuthFields.contains(it.field) }}"
                    setSpecs.findAll { it -> !it.matched }.each {
                        println "Unmatched Spec: ${normaliseSubfields(it.subfields)}"
                    }
                    matchedFields.each { it ->
                        println "\t${it}"
                    }
                    println "misMatches:"
                    misMatchedFields.each { it ->
                        println "\t${it}"
                    }
                }

                /* setSpecs.each { it ->
                     println it.value
                     if (it.type == "personalName") {
                         if (doc.fields.'100'.subfields.a.first().first() == it.value) {
                             println "match!"
                         }
                         if(doc.fields.'700'.subfields.a.first().first() == it.value){
                             println "match!"
                         }
                     }
                 }*/
            }
            if (!isSuppressed(doc)) {
                String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)
                def id = LegacyIntegrationTools.generateId(oldStyleIdentifier)
                Map convertedData = setSpecs && setSpecs.size() > 1 && collection != 'bib' ?
                        s_marcFrameConverter.convert(doc, id, [oaipmhSetSpecs: setSpecs]) :
                        s_marcFrameConverter.convert(doc, id)
                Document document = new Document(convertedData)
                document.created = rowMap.created
                writeDocumentToLoadFile(document, collection)
            }
        }
    }

    static String getSubfieldValue(Map p, String s) {

        for (subfield in p.subfields) {
            String key = subfield.keySet()[0];
            if (key == s) {
                return subfield[key]
            }
        }
        return ""
    }

    static List getHoldOaipmhSetSpecs(def resultSet) {
        List specs = []
        /* if (collection == "bib") {
             int authId = resultSet.auth_id ?: 0
             if (authId > 0) {
                 specs.add("authority:${authId}")
             }
         } else if (collection == "hold") {*/
        if (resultSet.bib_id > 0)
            specs.add("bibid:${resultSet.bib_id}")
        if (resultSet.sigel)
            specs.add("location:${resultSet.sigel}")
        // }
        return specs
    }

    private static boolean isSuppressed(Map doc) {
        def fields = doc.get("fields")
        for (def field : fields) {
            if (field.get("599") != null) {
                def field599 = field.get("599")
                if (field599.get("subfields") != null) {
                    def subfields = field599.get("subfields")
                    for (def subfield : subfields) {
                        if (subfield.get("a").equals("SUPPRESSRECORD"))
                            return true;
                    }
                }
            }
        }
        return false;
    }

    private static String getControlNumber(Map doc) {
        def fields = doc.get("fields")
        for (def field : fields) {
            if (field.get("001") != null)
                return field.get("001");
        }
        return null
    }

    private static
    synchronized void writeDocumentToLoadFile(Document doc, String collection) {
        /* columns:

           id text not null unique primary key,
           data jsonb not null,
           collection text not null,
           changedIn text not null,
           changedBy text,
           checksum text not null,
           created timestamp with time zone not null default now(),
           modified timestamp with time zone not null default now(),
           deleted boolean default false

           */

        final char delimiter = '\t';
        final String nullString = "\\N";

        final delimiterString = new String(delimiter);

        List<String> identifiers = doc.recordIdentifiers;
        //print "w"
        // Write to main table file

        s_mainTableWriter.write(doc.shortId);
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(doc.dataAsString.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString));
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(collection.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString));
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write("vcopy");
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(nullString);
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(doc.checksum.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString));
        s_mainTableWriter.write(delimiter);
        s_mainTableWriter.write(doc.created);

        // remaining values have sufficient defaults.

        s_mainTableWriter.newLine();

        // Write to identifiers table file

        /* columns:
        id text not null,
        identifier text not null -- unique
        */

        for (String identifier : identifiers) {
            s_identifiersWriter.write(doc.shortId);
            s_identifiersWriter.write(delimiter);
            s_identifiersWriter.write(identifier);

            s_identifiersWriter.newLine();
        }
    }

}

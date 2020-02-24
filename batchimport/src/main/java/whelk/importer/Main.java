package whelk.importer;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.PushGateway;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709MarcRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.Templates;
import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Collections;
import java.util.Iterator;

public class Main
{
    private static XL s_librisXl = null;

    private static boolean verbose = false;

    private static List tempfiles = Collections.synchronizedList(new ArrayList<File>());

    // Metrics
    private final static String METRICS_PUSHGATEWAY = "metrics.libris.kb.se:9091";
    private final static CollectorRegistry registry = new CollectorRegistry();
    private final static Counter importedBibRecords = Counter.build()
            .name("batchimport_imported_bibliographic_records_count")
            .help("The total number of bibliographic records imported.")
            .register(registry);
    private final static Counter importedHoldRecords = Counter.build()
            .name("batchimport_imported_holding_records_count")
            .help("The total number of holding records imported.")
            .register(registry);
    private final static Counter encounteredMulBibs = Counter.build()
            .name("batchimport_encountered_mulbibs")
            .help("The total number of incoming records with more than one duplicate already in the system.")
            .register(registry);

    // Abort on unhandled exceptions, including those on worker threads.
    static
    {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable)
            {
                System.err.println("fatal: PANIC ABORT, unhandled exception:\n");
                throwable.printStackTrace();
                System.exit(-1);
            }
        });
    }

	public static void main(String[] args)
            throws Exception
	{

        // Normal importing operations
	Parameters parameters = new Parameters(args);
	verbose = parameters.getVerbose();

        s_librisXl = new XL(parameters);

        if (parameters.getPath() == null)
            importStream(System.in, parameters);

        else // A path was specified
        {
            File file = new File(parameters.getPath().toString());
            if (file.isDirectory())
            {

                File[] subFiles = file.listFiles();
                if (subFiles == null)
                    return;

                // Sort the files in the directory chronologically
                Arrays.sort(subFiles, new Comparator<File>() {
                    @Override
                    public int compare(File o1, File o2) {
                        if (o1.lastModified() < o2.lastModified())
                            return -1;
                        else if (o1.lastModified() > o2.lastModified())
                            return 1;
                        return 0;
                    }
                });

                for (File subFile : subFiles)
                {
                    if (!subFile.isDirectory())
                        importFile(subFile.toPath(), parameters);
                }
            }
            else // regular file (not directory)
            {
                importFile(file.toPath(), parameters);
            }
        }

        try
        {
            PushGateway pg = new PushGateway(METRICS_PUSHGATEWAY);
            pg.pushAdd(registry, "batch_import");
        } catch (Throwable e)
        {
            System.err.println("Metrics server connection failed. No metrics will be generated.");
        }
	if ( verbose ) {
            System.err.println("info: All done.");
	}
    }

    /**
     * Convenience method for turning files (paths) into reliable inputstreams and passing them along to importStream()
     */
    private static void importFile(Path path, Parameters parameters)
            throws Exception
    {
        System.err.println("info: Importing file: " + path.toString());
        try (ExclusiveFile file = new ExclusiveFile(path);
             InputStream fileInStream = file.getInputStream())
        {
            importStream(fileInStream, parameters);
        }
    }

    private static void importStream(InputStream inputStream, Parameters parameters)
            throws Exception
    {
        inputStream = streamAsXml(inputStream, parameters);

        // Apply any transforms specified on the command line.
        // The expectation is that after these transforms, the stream will consist of a series
        // of marcxml records that the jmarctools.MarcXmlRecordReader can read. The order of the records
        // is also expected to be; one bib record followed by any related holding records, after which
        // comes the next bib record and so on.

        for (Templates template : parameters.getTemplates())
        {
	    inputStream = transform(template.newTransformer(), inputStream);
            //inputStream = transform(transformer, inputStream);
        }

        int threadCount = 1;
        if (parameters.getRunParallel())
            threadCount = 2 * Runtime.getRuntime().availableProcessors();
        ThreadPool threadPool = new ThreadPool( threadCount );

        MarcXmlRecordReader reader = null;
        try
        {
            reader = new MarcXmlRecordReader(inputStream, "/collection/record", null);

            long start = System.currentTimeMillis();
            long recordsBatched = 0;
            int recordsInBatch = 0;

            // Assemble a batch of records (= N * (one bib followed by zero or more hold) )
            MarcRecord marcRecord;
            List<MarcRecord> batch = new ArrayList<>();
            while ((marcRecord = reader.readRecord()) != null)
            {
                String collection = "bib"; // assumption
                if (marcRecord.getLeader(6) == 'u' || marcRecord.getLeader(6) == 'v' ||
                        marcRecord.getLeader(6) == 'x' || marcRecord.getLeader(6) == 'y')
                    collection = "hold";

                if (collection.equals("bib"))
                {
                    if (recordsInBatch > 200)
                    {
                        threadPool.executeOnThread(batch, Main::importBatch);
                        batch = new ArrayList<>();
                        recordsInBatch = 0;
                    }
                }
                batch.add(marcRecord);
                ++recordsBatched;
                ++recordsInBatch;

                if (recordsBatched % 100 == 0)
                {
                    long secondDiff = (System.currentTimeMillis() - start) / 1000;
                    if (secondDiff > 0)
                    {
                        long recordsPerSec = recordsBatched / secondDiff;
	    		if ( verbose ) {
                        	System.err.println("info: Currently importing " + recordsPerSec + " records / sec. Active threads: " + threadPool.getActiveThreadCount());
			}
                    }
                }
            }
            // The last batch will not be followed by another bib.
            threadPool.executeOnThread(batch, Main::importBatch);
        }
        finally
        {
            if (reader != null)
                reader.close();
            threadPool.joinAll();

        }

	inputStream.close();
	removeTemporaryFiles();
    }

    private static void removeTemporaryFiles()
    {
	synchronized(tempfiles) {
		Iterator<File> i = tempfiles.iterator();
		while (i.hasNext()) {
    			File f = i.next();
    			f.delete();
    			i.remove();
		}
	}

    }

    private static void importBatch(List<MarcRecord> batch)
    {
        String lastKnownBibDocId = null;
        for (MarcRecord marcRecord : batch)
        {
            try
            {
		if ( verbose ) {
			String ids[][] = DigId.digIds(marcRecord);
			if ( ids != null ) {
				for (String r[] : ids ) {
					if ( r != null ) {
						for (String c : r) {
							if ( c != null ) {
								System.out.printf("%s ", c);
							}
						}
					}
				}
			}
			System.out.println();
		}
                String resultingId = s_librisXl.importISO2709(
                        marcRecord,
                        lastKnownBibDocId,
                        importedBibRecords,
                        importedHoldRecords,
                        encounteredMulBibs);
                if (resultingId != null)
                    lastKnownBibDocId = resultingId;
            } catch (Exception e)
            {
                System.err.println("Failed to convert or write the following MARC post:\n" + marcRecord.toString());
                throw new RuntimeException(e);
            }
        }
    }

    private static File getTemporaryFile()
            throws IOException
    {
        File tempFile = File.createTempFile("xlimport", ".tmp");
	tempfiles.add(tempFile);
        return tempFile;
    }

    /**
     * Convert inputStream into a new InputStream which consists of XML records, regardless of the
     * original format.
     */
    private static InputStream streamAsXml(InputStream inputStream, Parameters parameters)
            throws IOException
    {
        if (parameters.getFormat() == Parameters.INPUT_FORMAT.FORMAT_ISO2709)
        {
            File tmpFile = getTemporaryFile();

            try(OutputStream tmpOut = new FileOutputStream(tmpFile))
            {
                Iso2709MarcRecordReader isoReader;
                isoReader = new Iso2709MarcRecordReader(inputStream, parameters.getInputEncoding());

                MarcXmlRecordWriter writer = new MarcXmlRecordWriter(tmpOut);

                MarcRecord marcRecord;
                while ((marcRecord = isoReader.readRecord()) != null)
                {
                    writer.writeRecord(marcRecord);
                }
                isoReader.close();
                writer.close();
                inputStream.close();
            }

            return new FileInputStream(tmpFile);
        } else // Already xml
            return inputStream;
    }

    /**
     * Apply a transformation XSLT stylesheet on everything available in inputStream
     * and generate a new InputStream of the transformed data.
     */
    private static InputStream transform(Transformer transformer, InputStream inputStream)
            throws TransformerException, IOException
    {
        File tmpFile = getTemporaryFile();
        try(OutputStream tmpOut = new FileOutputStream(tmpFile))
        {
            StreamResult result = new StreamResult(tmpOut);
            transformer.transform( new StreamSource(inputStream), result );
            inputStream.close();
        }
        return new FileInputStream(tmpFile);
    }
}

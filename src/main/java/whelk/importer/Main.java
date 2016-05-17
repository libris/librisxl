package whelk.importer;

import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709MarcRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.nio.file.Path;
import java.util.List;

public class Main
{
    // Abort on unhandled exceptions, including those on worker threads.
    static
    {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable)
            {
                System.err.println("PANIC ABORT, unhandled exception:\n");
                throwable.printStackTrace();
                System.exit(-1);
            }
        });
    }

	public static void main(String[] args)
            throws Exception
	{
        // Enrich a jsonld file with another jsonld file and print result. Mainly for testing purposes, imports nothing.
        if (args[0].equals("--enrichFiles"))
        {
            CliEnrich.enrich(args[1], args[2]);
            return;
        }

        // Normal importing operations
		Parameters parameters = new Parameters(args);

        if (parameters.getPath() == null)
            importStream(System.in, parameters);

        else // A path was speficied
        {
            File file = new File(parameters.getPath().toString());
            if (file.isDirectory())
            {
                for (File subFile : file.listFiles())
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

        System.err.println("All done.");
    }

    /**
     * Convenience method for turning files (paths) into reliable inputstreams and passing them along to importStream()
     */
    private static void importFile(Path path, Parameters parameters)
            throws Exception
    {
        System.err.println("Importing file: " + path.toString());
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

        // Apply any transforms
        List<Transformer> transformers = parameters.getTransformers();
        for (Transformer transformer : transformers)
            inputStream = transform(transformer, inputStream);

        XL librisXl = new XL(parameters);

        MarcXmlRecordReader reader = null;
        try
        {
            reader = new MarcXmlRecordReader(inputStream, "/collection/record", null);
            MarcRecord marcRecord;
            String lastKnownBibDocId = null;
            while ((marcRecord = reader.readRecord()) != null)
            {
                String resultingId = librisXl.importISO2709(marcRecord, lastKnownBibDocId);
                if (resultingId != null)
                    lastKnownBibDocId = resultingId;
            }
        }
        finally
        {
            if (reader != null)
                reader.close();
        }
    }

    private static File getTemporaryFile()
            throws IOException
    {
        File tempFile = File.createTempFile("xlimport", ".tmp");
        tempFile.deleteOnExit();
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

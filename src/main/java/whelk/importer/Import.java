package whelk.importer;

/* lxl_import, Synopsis

0. Setup configuration variables for channel
1. Choose reader for channel (xml, iso2709, json, json-ld,tsv)
2. Transform (if necessary, possibly to several records)
3. For each record in (transformed) stream:
4. Select whelk-id's (for duplicate check)
5. Depending on id's (what if multiple?) and recordtype (bib,hold,auth)
6. Action=delete,merge,add,replace (from marc-leader? or config?)

*/

import org.apache.commons.io.IOUtils;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709MarcRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;

public class Import
{
	public static void main(String[] args)
            throws Exception
	{
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
        inputStream = debug_printStreamToEnd(inputStream, "Before XML conversion");
        inputStream = streamAsXml(inputStream, parameters);
        inputStream = debug_printStreamToEnd(inputStream, "After XML conversion");

        // Apply any transforms
        List<Transformer> transformers = parameters.getTransformers();
        for (Transformer transformer : transformers)
            inputStream = transform(transformer, inputStream);
        
        MarcRecord marcRecord = new MarcXmlRecordReader(inputStream, "/collection/record", null).readRecord();
        if (marcRecord == null)
        {
            System.err.println("Input did not satisfy MarcXmlRecordReader! (returned null).");
        }

        importISO2709(marcRecord, parameters);
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
            ByteArrayOutputStream buf = new ByteArrayOutputStream();

            Iso2709MarcRecordReader isoReader;
            if (parameters.getInputEncoding() == null)
            {
                isoReader = new Iso2709MarcRecordReader(inputStream);
            } else
            {
                isoReader = new Iso2709MarcRecordReader(inputStream, parameters.getInputEncoding());
            }

            MarcXmlRecordWriter writer = new MarcXmlRecordWriter(buf);

            MarcRecord marcRecord;
            while ((marcRecord = isoReader.readRecord()) != null)
            {
                writer.writeRecord(marcRecord);
            }
            writer.close();
            inputStream.close();

            return new ByteArrayInputStream(buf.toByteArray());
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
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        StreamResult result = new StreamResult(buf);
        transformer.transform( new StreamSource(inputStream), result );
        inputStream.close();
        return new ByteArrayInputStream(buf.toByteArray());
    }

    /**
     * Write a ISO2709 MarcRecord to LibrisXL
     */
    private static void importISO2709(MarcRecord marcRecord, Parameters parameters)
            throws Exception
    {
        System.out.println("Would now import iso2709 record: " + marcRecord.toString());
    }

    private static InputStream debug_printStreamToEnd(InputStream inputStream, String description)
            throws IOException
    {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        IOUtils.copy(inputStream, buf);
        inputStream.close();
        System.err.println("Stream contents (" + description + "):\n" + new String(buf.toByteArray(), Charset.forName("UTF-8")));
        return new ByteArrayInputStream(buf.toByteArray());
    }
}

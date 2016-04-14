package whelk.importer;

/* lxl_import, Synopsis

0. Setup configuration variables for channel
1. Choose reader for channel
2. For each record in stream:
3. Select whelk-id's (for duplicate check)
4. Depending on id's and recordtype
5. Transform (if necessary)
6. Action=delete,merge,add (from marc-leader?)

*/

import org.apache.commons.io.IOUtils;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709Deserializer;
import se.kb.libris.util.marc.io.Iso2709Serializer;
import se.kb.libris.util.marc.io.MarcRecordReader;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
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
            else // regular file
            {
                importFile(file.toPath(), parameters);
            }
        }
    }

    private static void importFile(Path path, Parameters parameters)
            throws Exception
    {
        System.out.println("Doing file: " + path.toString());
        try (ExclusiveFile file = new ExclusiveFile(path);
             InputStream fileInStream = file.getInputStream())
        {
            importStream(fileInStream, parameters);
        }
    }

    private static void importStream(InputStream inputStream, Parameters parameters)
            throws Exception
    {
        MarcRecord marcRecord = null;

        switch (parameters.getFormat())
        {
            case FORMAT_XML:
            {
                // Apply any transforms
                List<Transformer> transformers = parameters.getTransformers();
                if (!transformers.isEmpty())
                {
                    for (Transformer transformer : transformers)
                        inputStream = transform(transformer, inputStream);
                }

                marcRecord = new MarcXmlRecordReader(inputStream).readRecord();
                break;
            }

            case FORMAT_ISO2709:
            {
                byte[] iso2709record = IOUtils.toByteArray(inputStream);
                marcRecord = Iso2709Deserializer.deserialize(iso2709record);
            }
        }

        importISO2709(marcRecord, parameters);
    }

    private static InputStream transform(Transformer transformer, InputStream inputStream)
            throws TransformerException
    {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        StreamResult result = new StreamResult(buf);
        transformer.transform( new StreamSource(inputStream), result );
        return new ByteArrayInputStream(buf.toByteArray());
    }

    private static void importISO2709(MarcRecord marcRecord, Parameters parameters)
            throws Exception
    {
        System.out.println("Would now import iso2709 record: " + marcRecord.toString());
    }
}

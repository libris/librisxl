package whelk.importer;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

class Parameters
{
    private Path path;
    private INPUT_FORMAT format;
    private boolean readOnly = true;
    private List<Transformer> transformers = new ArrayList<>();
    private List<DUPLICATION_TYPE> dupTypes = new ArrayList<>();
    private String inputEncoding = "UTF-8";

    Path getPath() { return path; }
    INPUT_FORMAT getFormat() { return format; }
    boolean getReadOnly() { return readOnly; }
    List<Transformer> getTransformers() { return transformers; }
    List<DUPLICATION_TYPE> getDuplicationTypes() { return dupTypes; }
    String getInputEncoding() { return inputEncoding; }

    enum INPUT_FORMAT
    {
        FORMAT_ISO2709,
        FORMAT_XML,
    }

    enum DUPLICATION_TYPE
    {
        DUPTYPE_ISBNA,
        DUPTYPE_ISBNZ,
        DUPTYPE_ISSNA,
        DUPTYPE_ISSNZ,
        DUPTYPE_035A,
        DUPTYPE_LIBRISID
    }

    Parameters(String[] args)
            throws Exception
    {
        for (String arg : args)
        {
            int separatorIndex= arg.indexOf('=');
            if (separatorIndex > 1 && arg.length() > 2)
            {
                String param = arg.substring(0, separatorIndex);
                String value = arg.substring(separatorIndex+1);
                interpretBinaryParameter(param, value);
            }
            else
            {
                interpretUnaryParameter(arg);
            }
        }

        // Check required parameters
        if (format == null)
        {
            printUsage();
            System.exit(-1);
        }
    }

    private void printUsage()
    {
        System.err.println("Usage: ..");
    }

    private void interpretBinaryParameter(String parameter, String value)
            throws Exception
    {
        switch (parameter)
        {
            case "--path":
                path = Paths.get(value);
                break;

            case "--format":
                switch (value)
                {
                    case "iso2709":
                        format = INPUT_FORMAT.FORMAT_ISO2709;
                        break;
                    case "xml":
                        format = INPUT_FORMAT.FORMAT_XML;
                        break;
                    default:
                        throw new IllegalArgumentException(value + " is not a valid indata format.");
                }
                break;

            case "--transformer":
                transformers.add( TransformerFactory.newInstance().newTransformer(
                        new StreamSource(new File(value))) );
                break;

            case "--inEncoding": // Only relevant for non-xml formats. XML files are expected to declare encoding in their header.
                inputEncoding = value;
                break;

            case "--dupType":
                String[] types = value.split(",");
                for (String type : types)
                    dupTypes.add(translateDuplicationType(type));
                break;

            default:
                throw new IllegalArgumentException(parameter);
        }
    }

    private DUPLICATION_TYPE translateDuplicationType(String typeString)
    {
        switch (typeString)
        {
            case "ISBNA":
                return DUPLICATION_TYPE.DUPTYPE_ISBNA;
            case "ISBNZ":
                return DUPLICATION_TYPE.DUPTYPE_ISBNZ;
            case "ISSNA":
                return DUPLICATION_TYPE.DUPTYPE_ISSNA;
            case "ISSNZ":
                return DUPLICATION_TYPE.DUPTYPE_ISSNZ;
            case "035A":
                return DUPLICATION_TYPE.DUPTYPE_035A;
            case "LIBRIS-ID":
                return DUPLICATION_TYPE.DUPTYPE_LIBRISID;
        }
        throw new IllegalArgumentException(typeString + " is not a valid value duplication type.");
    }

    private void interpretUnaryParameter(String parameter)
    {
        switch (parameter)
        {
            case "--live":
                readOnly = false;
                break;
            default:
                throw new IllegalArgumentException(parameter);
        }
    }
}

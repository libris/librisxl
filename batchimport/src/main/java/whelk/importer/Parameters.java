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
    private boolean parallel = false;
    private boolean enrichMulDup = false;

    Path getPath() { return path; }
    INPUT_FORMAT getFormat() { return format; }
    boolean getReadOnly() { return readOnly; }
    List<Transformer> getTransformers() { return transformers; }
    List<DUPLICATION_TYPE> getDuplicationTypes() { return dupTypes; }
    String getInputEncoding() { return inputEncoding; }
    boolean getRunParallel() { return parallel; }
    boolean getEnrichMulDup() { return enrichMulDup; }

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
        System.err.println("Usage: java -Dxl.secret.properties=PROPSFILE -jar lxl_import.jar [PARAMETERS] [DATA]");
        System.err.println("");
        System.err.println("Imports records into Libris XL.");
        System.err.println("");
        System.err.println("In order to function, this program needs a Libris XL secret properties file. Please consult");
        System.err.println("the whelk-core readme, for information on how to build such a file");
        System.err.println("");
        System.err.println("This program expects records fed to it to have a certain order, such that each bibliograhic");
        System.err.println("record be followed by the holding records for that record (if any). This ordering of the");
        System.err.println("data must be in effect at the latest after any XSLT transforms have been applied. This");
        System.err.println("program should NOT be used to import authority records.");
        System.err.println("");
        System.err.println("Parameters:");
        System.err.println("");
        System.err.println("--path        A file path to use as input data. If this path represents a folder, all");
        System.err.println("              files in that folder will be imported (but no subfolders).");
        System.err.println("              If the path represents a file, only that specific file will be imported.");
        System.err.println("              If this parameter is omitted, lxl_import will expect its input on stdin.");
        System.err.println("");
        System.err.println("--format      The format of the input data. Can be either \"iso2709\" or \"xml\".");
        System.err.println("              This parameter must be specified. If the format is \"xml\", the structure of");
        System.err.println("              the xml document must be that of MARCXML at the latest after any XSLT");
        System.err.println("              transforms have been applied.");
        System.err.println("");
        System.err.println("--transformer The path to an XSLT stylsheet that should be used to transform the input");
        System.err.println("              before importing. This parameter may be used even if the input format is");
        System.err.println("              \"iso2709\", in which case the stream will be translated to MARCXML before");
        System.err.println("              transformation. If more than one transformer is specified these will be");
        System.err.println("              applied in the same order they are specified on the command line.");
        System.err.println("              XSLT transformation is optional.");
        System.err.println("");
        System.err.println("--inEncoding  The character encoding of the incoming data. Only relevant if the format");
        System.err.println("              is \"iso2709\" as xml documents are expected to declare their encoding in the");
        System.err.println("              xml header. Defaults to UTF-8.");
        System.err.println("");
        System.err.println("--dupType     The type of duplication checking that should be done for each incoming");
        System.err.println("              record. The value of this parameter may be a comma-separated list of any");
        System.err.println("              combination of duplication types. If a duplicate is found for an incoming");
        System.err.println("              record, that record will be enriched with any additional information in the");
        System.err.println("              incoming record.");
        System.err.println("              Duplication types:");
        System.err.println("                ISBNA     ISBN number, obtained from MARC subfield $a of the incoming record");
        System.err.println("                ISBNZ     ISBN number, obtained from MARC subfield $z of the incoming record");
        System.err.println("                ISSNA     ISSN number, obtained from MARC subfield $a of the incoming record");
        System.err.println("                ISSNZ     ISSN number, obtained from MARC subfield $z of the incoming record");
        System.err.println("                035A      ID in other system, obtained from MARC 035 $a of the incoming record");
        System.err.println("                LIBRIS-ID ID in Libris.");
        System.err.println("");
        System.err.println("--live        Write to Whelk (without this flag operations against the Whelk are readonly");
        System.err.println("              and results are only printed to stdout).");
        System.err.println("");
        System.err.println("--parallel    Do document conversion, enrichment and duplicate checking in parallel. Use with");
        System.err.println("              care. Specifically do not use for sources where multiples of the same document");
        System.err.println("              may appear in one batch. If you do, you risk introducing multiples in the");
        System.err.println("              database, because there is no synchronization in between duplicate checks");
        System.err.println("              and writing a document.");
        System.err.println("");
        System.err.println("--enrichMulDup If duplication checking finds more than one duplicate for an incoming");
        System.err.println("              document, the incoming document is normally ignored/skipped. If this flag is");
        System.err.println("              set however, all found duplicates will be enriched with the information from");
        System.err.println("              the incoming record.");
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
            case "--parallel":
                parallel = true;
                break;
            case "--enrichMulDup":
                enrichMulDup = true;
                break;
            default:
                throw new IllegalArgumentException(parameter);
        }
    }
}

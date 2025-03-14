package whelk.importer;

import javax.xml.transform.Templates;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

class Parameters
{
    private Path path;
    private INPUT_FORMAT format;
    private boolean readOnly = true;
    private final List<Templates> templates = new ArrayList<>();
    private final List<DUPLICATION_TYPE> dupTypes = new ArrayList<>();
    private String inputEncoding = "UTF-8";
    private boolean parallel = false;
    private boolean verbose = false;
    private boolean replaceHold = false;
    private boolean replaceBib = false;
    private boolean ignoreNewBib = false;
    private String changedBy = null;
    private String changedIn = null;
    private boolean forceUpdate = false;
    private final HashMap<String, Set<String>> specialRules = new HashMap<>();
    private Path mergeRuleFilePath = null;
    private boolean ignoreHistory = false;

    Path getPath() { return path; }
    INPUT_FORMAT getFormat() { return format; }
    boolean getReadOnly() { return readOnly; }
    List<Templates> getTemplates() { return templates; }
    List<DUPLICATION_TYPE> getDuplicationTypes() { return dupTypes; }
    String getInputEncoding() { return inputEncoding; }
    boolean getRunParallel() { return parallel; }
    boolean getVerbose() { return verbose; }
    boolean getReplaceHold() { return replaceHold; }
    boolean getReplaceBib() { return replaceBib; }
    boolean getIgnoreNewBib() { return ignoreNewBib; }
    String getChangedBy() { return changedBy; }
    String getChangedIn() { return changedIn; }
    boolean getForceUpdate() { return forceUpdate; }
    HashMap<String, Set<String>> getSpecialRules() { return specialRules; }
    Path getMergeRuleFile() { return mergeRuleFilePath; }
    boolean getIgnoreHistory() { return ignoreHistory; }


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
        DUPTYPE_LIBRISID,
        DUPTYPE_EAN,
        DUPTYPE_URI,
        DUPTYPE_URN,
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
        if (format == null || changedBy == null)
        {
            printUsage();
            System.exit(-1);
        }
    }

    private void printUsage()
    {
        System.err.println("Usage: java -Dxl.secret.properties=PROPSFILE -jar batchimport.jar [PARAMETERS] [DATA]");
        System.err.println();
        System.err.println("Imports records into Libris XL.");
        System.err.println();
        System.err.println("In order to function, this program needs a Libris XL secret properties file. Please consult");
        System.err.println("the whelk-core readme, for information on how to build such a file");
        System.err.println();
        System.err.println("This program expects records fed to it to have a certain order, such that each bibliograhic");
        System.err.println("record be followed by the holding records for that record (if any). This ordering of the");
        System.err.println("data must be in effect at the latest after any XSLT transforms have been applied. This");
        System.err.println("program should NOT be used to import authority records.");
        System.err.println();
        System.err.println("Parameters:");
        System.err.println();
        System.err.println("--path        A file path to use as input data. If this path represents a folder, all");
        System.err.println("              files in that folder will be imported (but no subfolders).");
        System.err.println("              If the path represents a file, only that specific file will be imported.");
        System.err.println("              If this parameter is omitted, lxl_import will expect its input on stdin.");
        System.err.println();
        System.err.println("--format      The format of the input data. Can be either \"iso2709\" or \"xml\".");
        System.err.println("              This parameter must be specified. If the format is \"xml\", the structure of");
        System.err.println("              the xml document must be that of MARCXML at the latest after any XSLT");
        System.err.println("              transforms have been applied.");
        System.err.println();
        System.err.println("--transformer The path to an XSLT stylesheet that should be used to transform the input");
        System.err.println("              before importing. This parameter may be used even if the input format is");
        System.err.println("              \"iso2709\", in which case the stream will be translated to MARCXML before");
        System.err.println("              transformation. If more than one transformer is specified these will be");
        System.err.println("              applied in the same order they are specified on the command line.");
        System.err.println("              XSLT transformation is optional.");
        System.err.println();
        System.err.println("--inEncoding  The character encoding of the incoming data. Only relevant if the format");
        System.err.println("              is \"iso2709\" as xml documents are expected to declare their encoding in the");
        System.err.println("              xml header. Defaults to UTF-8.");
        System.err.println();
        System.err.println("--dupType     The type of duplication checking that should be done for each incoming");
        System.err.println("              record. The value of this parameter may be a comma-separated list of any");
        System.err.println("              combination of duplication types.");
        System.err.println("              Duplication types are checked in the order they are defined on the command line");
        System.err.println("              As soon as one duplication type has yielded one or more duplicates, no additional");
        System.err.println("              types will be checked. (in other words: THE ORDERING OF TYPES MATTER)");
        System.err.println("              Duplication types:");
        System.err.println("                ISBNA     ISBN number, obtained from MARC subfield $a of the incoming record");
        System.err.println("                ISBNZ     ISBN number, obtained from MARC subfield $z of the incoming record");
        System.err.println("                ISSNA     ISSN number, obtained from MARC subfield $a of the incoming record");
        System.err.println("                ISSNZ     ISSN number, obtained from MARC subfield $z of the incoming record");
        System.err.println("                035A      ID in other system, obtained from MARC 035 $a of the incoming record");
        System.err.println("                EAN       ID in other system, obtained from MARC 024 $a ind0 = 3 of the incoming record");
        System.err.println("                URI       ID in other system, obtained from MARC 024 $a ind0 = 7 & $2 = uri of the incoming record");
        System.err.println("                URN       ID in other system, obtained from MARC 024 $a ind0 = 7 & $2 = urn of the incoming record");
        System.err.println("                LIBRIS-ID ID in Libris.");
        System.err.println();
        System.err.println("--live        Write to Whelk (without this flag operations against the Whelk are readonly");
        System.err.println("              and results are only printed to stdout).");
        System.err.println();
        System.err.println("--parallel    Do document conversion, enrichment and duplicate checking in parallel. Use with");
        System.err.println("              care. Specifically do not use for sources where multiples of the same document");
        System.err.println("              may appear in one batch. If you do, you risk introducing multiples in the");
        System.err.println("              database, because there is no synchronization in between duplicate checks");
        System.err.println("              and writing a document.");
        System.err.println();
        System.err.println("--verbose     Verbose logging.");
        System.err.println();
        System.err.println("--replaceBib  If this flag is set, matching bibliographic records will be replaced.");
        System.err.println();
        System.err.println("--ignoreNewBib If this flag is set, no _new_ bibliographic records will be created");
        System.err.println("              (existing ones may still optionally be replaced/merged).");
        System.err.println("              any holding records associated with an incoming bibliographic record that did not match");
        System.err.println("              and which cannot be created, because of this flag, will be discarded.");
        System.err.println();
        System.err.println("--replaceHold If this flag is set, matching holding records will be replaced.");
        System.err.println();
        System.err.println("--mergeBibUsing Bibliographic records can be \"partially updated\" (merged) with incoming");
        System.err.println("              records according to some specified set of rules.");
        System.err.println("              Using this option, a file containing such merge rules can be specified.");
        System.err.println();
        System.err.println("--ignoreHistory When using --mergeBibUsing, the \"ownership\" of various parts of the record");
        System.err.println("              is taken into account. Hand edited things remain locked, and there can be an");
        System.err.println("              order of priority with regards to \"whose\" edits we trust more. But when this");
        System.err.println("              flag is specified, we ignore the existing history of all records, and apply");
        System.err.println("              merge rules, as if all existing data had the lowest possible priority (0).");
        System.err.println();
        System.err.println("--changedBy   A string to use as descriptionCreator (MARC 040) for imported records.");
        System.err.println("              This parameter must be specified.");
        System.err.println("--changedIn   A string to use for the changedIn column, defaults to \"batch import\".");
        System.err.println("--forceUpdate If this option is passed, encoding levels are ignored when deciding if");
        System.err.println("              An incoming record is allowed to affect existing records. USE WITH CARE");
        System.err.println("              and _NEVER_ use automatically.");
        System.err.println("--specialRule Use a non-standard rule for encoding level update permissions.");
        System.err.println("              The rule is passed as a string like so:");
        System.err.println("                --specialRule=7+3");
        System.err.println("              The above example will allow records with encodingLevel 7 to affect");
        System.err.println("              records with encodingLevel 3 (which would normally not be allowed).");
        System.err.println("              Several rules may be specified. This only affects the allowed import");
        System.err.println("              levels (3,5,7,8).");
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
                templates.add( TransformerFactory.newInstance().newTemplates( new StreamSource(new File(value))) );
                break;

            case "--inEncoding": // Only relevant for non-xml formats. XML files are expected to declare encoding in their header.
                inputEncoding = value;
                break;

            case "--changedBy":
                changedBy = value;
                break;

            case "--changedIn":
                changedIn = value;
                break;

            case "--dupType":
                String[] types = value.split(",");
                for (String type : types)
                    dupTypes.add(translateDuplicationType(type));
                break;

            case "--specialRule":
                if (value.length() != 3 || value.charAt(1) != '+')
                    throw new IllegalArgumentException(parameter);
                String from = translateEncodingLevel(value.charAt(0));
                String to = translateEncodingLevel(value.charAt(2));
                if (from == null || to == null)
                    throw new IllegalArgumentException(parameter);
                if ( !specialRules.containsKey(from) ) {
                    specialRules.put(from, new HashSet<String>()); //enumset?
                }
                //specialRules.put(from, to);
                specialRules.get(from).add(to);
                break;

            case "--mergeBibUsing":
                if (getReplaceBib())
                    throw new IllegalArgumentException("Encrich/replace are mutually exclusive");
                mergeRuleFilePath = Path.of(value);
                break;

            default:
                throw new IllegalArgumentException(parameter);
        }
    }

    private String translateEncodingLevel(char marcLevel)
    {
        switch (marcLevel)
        {
            case '3':
                return XL.ENC_ABBREVIVATED_STATUS;
            case '5':
                return XL.ENC_PRELIMINARY_STATUS;
            case '7':
                return XL.ENC_MINMAL_STATUS;
            case '8':
                return XL.ENC_PREPUBLICATION_STATUS;
            default:
                return null;
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
            case "EAN":
                return DUPLICATION_TYPE.DUPTYPE_EAN;
            case "URI":
                return DUPLICATION_TYPE.DUPTYPE_URI;
            case "URN":
                return DUPLICATION_TYPE.DUPTYPE_URN;
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
            case "--verbose":
                verbose = true;
                break;
            case "--replaceHold":
                replaceHold = true;
                break;
            case "--replaceBib":
                if (getMergeRuleFile() != null)
                    throw new IllegalArgumentException("Encrich/replace are mutually exclusive");
                replaceBib = true;
                break;
            case "--ignoreNewBib":
                ignoreNewBib = true;
                break;
            case "--forceUpdate":
                forceUpdate = true;
                break;
            case "--ignoreHistory":
                ignoreHistory = true;
                break;
            default:
                throw new IllegalArgumentException(parameter);
        }
    }
}

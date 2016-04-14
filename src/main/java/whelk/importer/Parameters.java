package whelk.importer;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Parameters
{
    private Path path;
    private INPUT_FORMAT format;
    private boolean readOnly = true;
    private List<Transformer> transformers = new ArrayList<Transformer>();
    private String inputEncoding;

    public Path getPath() { return path; }
    public INPUT_FORMAT getFormat() { return format; }
    public boolean getReadOnly() { return readOnly; }
    public List<Transformer> getTransformers() { return transformers; }
    public String getInputEncoding() { return inputEncoding; }

    public enum INPUT_FORMAT
    {
        FORMAT_ISO2709,
        FORMAT_XML,
    }

    public Parameters(String[] args)
            throws Exception
    {
        for (String arg : args)
        {
            int separatorIndex= arg.indexOf('=');
            if (separatorIndex > 1 && arg.length() > 2)
            {
                String param = arg.substring(0, separatorIndex);
                String value = arg.substring(separatorIndex+1);
                System.out.println(param + " -> " + value);
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
        System.out.println("Usage: ..");
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

            case "--inEncoding":
                inputEncoding = value;
                break;

            default:
                throw new IllegalArgumentException(parameter);
        }
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

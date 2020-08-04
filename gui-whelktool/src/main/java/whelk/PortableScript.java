package whelk;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

public class PortableScript implements Serializable
{
    final String scriptText;
    final Set<String> ids;
    final public String comment;

    public PortableScript(String scriptText, Set<String> ids, String comment)
    {
        this.scriptText = scriptText;
        if (ids != null)
            this.ids = Collections.unmodifiableSet(ids);
        else
            this.ids = null;
        this.comment = comment;
    }

    public Path execute() throws IOException
    {
        Path scriptWorkingDir = Files.createTempDirectory("xl_script");
        Path scriptFilePath = scriptWorkingDir.resolve("script.groovy");
        Path inputFilePath = scriptWorkingDir.resolve("input");
        Path reportPath = scriptWorkingDir.resolve("report");
        Files.createDirectories(reportPath);

        String flattenedScriptText = scriptText;
        if (ids != null)
        {
            Files.write(inputFilePath, ids);
            flattenedScriptText = scriptText.replace("£INPUT", inputFilePath.toString());
        }
        Files.write(scriptFilePath, flattenedScriptText.getBytes());

        String[] args =
                {
                        "--allow-loud",
                        "--report",
                        reportPath.toString(),
                        scriptFilePath.toString(),
                };

        whelk.datatool.WhelkTool.main(args);

        return reportPath;
    }
}
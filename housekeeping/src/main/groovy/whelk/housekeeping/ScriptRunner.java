package whelk.housekeeping;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;
import whelk.datatool.WhelkTool;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;

public class ScriptRunner extends HouseKeeper {
    private String status = "OK";
    private final String scriptName;
    private final String schedule;
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final Whelk premadeWhelk;

    public ScriptRunner(Whelk premadeWhelk, String scriptName, String schedule) {
        this.scriptName = scriptName;
        this.schedule = schedule;
        this.premadeWhelk = premadeWhelk;
    }

    public String getName() {
        return "Scriptrunner: " + scriptName;
    }

    public String getStatusDescription() {
        return status;
    }

    public String getCronSchedule() {
        return schedule;
    }

    public void trigger() {
        try {
            InputStream scriptStream = ScriptRunner.class.getClassLoader().getResourceAsStream(scriptName);
            String scriptText = IOUtils.toString(new InputStreamReader(scriptStream));

            Path scriptWorkingDir = Files.createTempDirectory("housekeeping_script_execution");
            Path scriptFilePath = scriptWorkingDir.resolve("script.groovy");
            Path reportPath = scriptWorkingDir.resolve("report");
            Files.createDirectories(reportPath);

            Files.writeString(scriptFilePath, scriptText);
            String[] args =
                    {
                            "--allow-loud",
                            "--report",
                            reportPath.toString(),
                            scriptFilePath.toString(),
                    };
            WhelkTool.main2(args, premadeWhelk);

            Path errorLogPath = reportPath.resolve("ERRORS.txt");
            if (Files.size(errorLogPath) > 0) {
                String firstError = "[could not read]";
                try (BufferedReader errorLogReader = Files.newBufferedReader(errorLogPath)) {
                    firstError = errorLogReader.readLine();
                }
                String errorMessage = "Execution of script (" + scriptName +  ") resulted in non-zero size ERRORS.txt, first line of which is: "
                        + firstError + "\nSee: " + errorLogPath;
                logger.error(errorMessage);
                status = errorMessage;
            } else {
                status = "OK";
            }

        } catch (Exception e) {
            logger.error("Failed script (" + scriptName + ") execution attempt: ", e);
            throw new RuntimeException(e);
        }

    }
}
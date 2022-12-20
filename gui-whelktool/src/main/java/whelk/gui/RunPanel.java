package whelk.gui;

import whelk.PortableScript;
import whelk.util.PropertyLoader;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

public class RunPanel extends WizardCard implements ActionListener
{
    private final JTextArea stdErrArea;
    private final JScrollPane stdErrScroll;
    private final JPasswordField passwordField;
    private final JButton startButton;

    private PortableScript scriptToRun;

    private final AtomicBoolean scriptIsDone = new AtomicBoolean(false);

    public RunPanel(Wizard wizard)
    {
        super(wizard);

        stdErrArea = new JTextArea();
        stdErrArea.setFont(new Font("monospaced", Font.PLAIN, 12));
        stdErrScroll = new JScrollPane(stdErrArea);
        stdErrScroll.setPreferredSize(new Dimension(300, 300));
        stdErrArea.setEditable(false);

        Box stdErrBox = Box.createVerticalBox();
        stdErrBox.add(new JLabel("Körningslogg:"));
        stdErrBox.add(stdErrScroll);
        this.add(stdErrBox);

        Box vbox = Box.createVerticalBox();

        vbox.add(new JLabel("Databasens lösenord:"));
        passwordField = new JPasswordField();
        vbox.add(passwordField);

        vbox.add(Box.createVerticalStrut(10));
        startButton = new JButton("Starta");
        startButton.setActionCommand("start");
        startButton.addActionListener(this);
        vbox.add(startButton);

        this.add(vbox);
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        scriptToRun = (PortableScript) parameterFromPreviousCard;
        disableNext();
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent)
    {
        if (actionEvent.getActionCommand().equals("start"))
        {
            startButton.setEnabled(false);
            passwordField.setEnabled(false);

            ByteArrayOutputStream stdErrStream = new ByteArrayOutputStream();
            System.setErr(new PrintStream(stdErrStream));

            disableNext();
            disableBack();
            disableCancel();

            new Thread(new Runnable() {
                public void run()
                {
                    try
                    {
                        if (System.getProperty("secretSqlUrl") != null)
                        {
                            String secretProperties = "sqlUrl = " +
                                    System.getProperty("secretSqlUrl").
                                            replace("_XL_PASSWORD_", new String(passwordField.getPassword())) + "\n" +
                                    "baseUri = " +
                                    System.getProperty("secretBaseUri") + "\n" +
                                    "elasticHost = " +
                                    System.getProperty("secretElasticHost") + "\n" +
                                    "elasticCluster = " +
                                    System.getProperty("secretElasticCluster") + "\n" +
                                    "elasticIndex = " +
                                    System.getProperty("secretElasticIndex") + "\n" +
                                    "applicationId = " +
                                    System.getProperty("secretApplicationId") + "\n" +
                                    "systemContextUri = " +
                                    System.getProperty("secretSystemContextUri") + "\n" +
                                    "locales = " +
                                    System.getProperty("secretLocales") + "\n" +
                                    "timezone = " +
                                    System.getProperty("secretTimezone") + "\n";

                            PropertyLoader.setUserEnteredProperties("secret", secretProperties);
                        }

                        Path reportDir = scriptToRun.execute();

                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        try (FileOutputStream os = new FileOutputStream(reportDir.resolve("Skapad " + dateFormat.format(new Date()) + " " + scriptToRun.comment + ".xlscript").toFile());
                             ObjectOutputStream oos = new ObjectOutputStream(os))
                        {
                            oos.writeObject(scriptToRun);
                        } catch (IOException ioe)
                        {
                            Wizard.exitFatal(ioe);
                        }

                        Desktop.getDesktop().open(reportDir.toFile());
                    } catch (IOException e) {
                        Wizard.exitFatal(e);
                    }

                    scriptIsDone.set(true);
                    System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
                    enableCancel();
                }
            }).start();

            new Thread(new Runnable() {
                public void run()
                {
                    try
                    {
                        while (!scriptIsDone.get())
                        {
                            Thread.sleep(500);
                            
                            stdErrArea.setText(stdErrStream.toString());
                            stdErrScroll.getVerticalScrollBar().setValue(Integer.MAX_VALUE);
                        }
                    } catch (InterruptedException e) { /* ignore */ }
                }
            }).start();
        }
    }
}

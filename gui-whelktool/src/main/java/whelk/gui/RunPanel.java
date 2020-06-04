package whelk.gui;

import whelk.PortableScript;
import whelk.Whelk;
import whelk.util.PropertyLoader;
import whelk.util.WhelkFactory;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

public class RunPanel extends WizardCard implements ActionListener {

    private Wizard wizard;

    private JTextArea stdErrArea;
    private JScrollPane stdErrScroll;
    private JPasswordField passwordField;
    private JButton startButton;

    private JButton saveButton;

    private PortableScript scriptToRun;

    private AtomicBoolean scriptIsDone = new AtomicBoolean(false);

    public RunPanel(Wizard wizard)
    {
        super(wizard);

        stdErrArea = new JTextArea();
        stdErrArea.setFont(new Font("monospaced", Font.PLAIN, 12));
        stdErrScroll = new JScrollPane(stdErrArea);
        stdErrScroll.setPreferredSize(new Dimension(300, 300));
        stdErrArea.setEditable(false);

        Box stdErrBox = Box.createVerticalBox();
        stdErrBox.add(new JLabel("stderr:"));
        stdErrBox.add(stdErrScroll);
        this.add(stdErrBox);

        Box vbox = Box.createVerticalBox();

        vbox.add(new JLabel("Database password:"));
        passwordField = new JPasswordField();
        vbox.add(passwordField);

        vbox.add(Box.createVerticalStrut(10));
        startButton = new JButton("Starta");
        startButton.setActionCommand("start");
        startButton.addActionListener(this);
        vbox.add(startButton);

        vbox.add(Box.createVerticalStrut(10));
        saveButton = new JButton("Spara");
        saveButton.setActionCommand("save");
        saveButton.addActionListener(this);
        saveButton.setEnabled(false);
        vbox.add(saveButton);



        this.add(vbox);
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        scriptToRun = (PortableScript) parameterFromPreviousCard;
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent)
    {
        if (actionEvent.getActionCommand().equals("save"))
        {
            JFileChooser fileChooser = new JFileChooser();
            fileChooser.setDialogTitle("Spara fil");

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String strDate = dateFormat.format(new Date());

            fileChooser.setSelectedFile(new File("Skapad " + dateFormat.format(new Date()) + " " + scriptToRun.comment + ".xlscript"));

            int result = fileChooser.showSaveDialog(wizard);
            if (result == JFileChooser.APPROVE_OPTION)
            {
                File saveFile = fileChooser.getSelectedFile();
                try (FileOutputStream os = new FileOutputStream(saveFile);
                     ObjectOutputStream oos = new ObjectOutputStream(os))
                {
                    oos.writeObject(scriptToRun);
                } catch (IOException ioe)
                {
                    Wizard.exitFatal(ioe.getMessage());
                }
            }
            enableCancel();
        }

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
                                System.getProperty("secretElasticIndex") + "\n";

                        PropertyLoader.setUserEnteredProperties("secret", new ByteArrayInputStream(secretProperties.getBytes()));

                        scriptToRun.execute();
                    } catch (IOException e) {
                        JOptionPane.showMessageDialog(null, e.getMessage(), "Message", JOptionPane.INFORMATION_MESSAGE);
                        Wizard.exitFatal(e.getMessage());
                    }

                    scriptIsDone.set(true);
                    saveButton.setEnabled(true);
                    System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
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
                            //stdErrScroll.getVerticalScrollBar().setValue(Integer.MAX_VALUE);
                        }
                    } catch (InterruptedException e) { /* ignore */ }
                }
            }).start();
        }
    }
}

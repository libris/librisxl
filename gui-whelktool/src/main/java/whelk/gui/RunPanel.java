package whelk.gui;

import whelk.Whelk;
import whelk.util.WhelkFactory;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class RunPanel extends WizardCard implements ActionListener {

    private JTextArea stdOutArea;
    private JScrollPane stdOutScroll;
    private JTextArea stdErrArea;
    private JScrollPane stdErrScroll;
    private JPasswordField passwordField;
    private JButton startButton;

    public RunPanel(Wizard wizard)
    {
        super(wizard);
        stdOutArea = new JTextArea();
        stdOutScroll = new JScrollPane(stdOutArea);
        stdOutScroll.setPreferredSize(new Dimension(300, 300));
        stdOutArea.setEditable(false);

        Box stdOutBox = Box.createVerticalBox();
        stdOutBox.add(new JLabel("stdout:"));
        stdOutBox.add(stdOutScroll);
        this.add(stdOutBox);

        stdErrArea = new JTextArea();
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

        this.add(vbox);
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        JOptionPane.showMessageDialog(null, System.getProperty("secretSqlUrl"), "Message", JOptionPane.INFORMATION_MESSAGE);
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent)
    {
        if (actionEvent.getActionCommand().equals("start"))
        {
            startButton.setEnabled(false);
            passwordField.setEnabled(false);

            ByteArrayOutputStream stdOutStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(stdOutStream));

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
                        //PropertyLoader.setUserEnteredProperties("secret", null);
                        Whelk whelk = WhelkFactory.getSingletonWhelk();

                        for (int i = 0; i < 25; ++i)
                        {
                            Thread.sleep(1000);
                            stdOutArea.append("yada.. " + i + "\n");
                            stdOutScroll.getVerticalScrollBar().setValue(Integer.MAX_VALUE);

                            stdErrArea.setText(stdErrStream.toString());
                            stdErrScroll.getVerticalScrollBar().setValue(Integer.MAX_VALUE);
                        }
                        enableCancel();
                    } catch (InterruptedException e) {

                    }

                    System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
                }
            }).start();
        }
    }
}

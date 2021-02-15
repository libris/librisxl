package whelk.gui;

import whelk.ScriptGenerator;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ChangeSigelPanel extends WizardCard implements DocumentListener, ActionListener
{
    final Wizard window;

    private JTextField fromSigelField;
    private JTextField toSigelField;
    private JFileChooser chooser = new JFileChooser();
    private File chosenFile;
    private JTextField chosenFileField;

    public ChangeSigelPanel(Wizard wizard)
    {
        super(wizard);
        window = wizard;

        chooser.setPreferredSize(new Dimension(1024, 768));

        Box vbox = Box.createVerticalBox();

        vbox.add(new JLabel("Sigel att byta ifrån"));
        fromSigelField = new JTextField();
        fromSigelField.getDocument().addDocumentListener(this);
        vbox.add(fromSigelField);

        vbox.add(Box.createVerticalStrut(10));

        vbox.add(new JLabel("Sigel att byta till"));
        toSigelField = new JTextField();
        toSigelField.getDocument().addDocumentListener(this);
        vbox.add(toSigelField);

        vbox.add(Box.createVerticalStrut(10));

        vbox.add(new JLabel("<html>Om du vill begränsa körningen till bestånd på specifika bilbiografiska IDn:<br/><br/>" +
                "Vänligen välj en fil med IDn.<br/><br/>" +
                "Filen måste innehålla libris bibliografiska kontrollnummer<br/>" +
                "motsvarande {@graph,0,controlNumber} eller MARC-fält 001.<br/><br/>" +
                "T ex: 1890245 eller tb4s55f50hm8w1f.<br/>Ett ID per rad.<br/><br/>" +
                "Väljer du ingen lista, så ändras samtliga bestånd med valt sigel.</html>"));

        vbox.add(Box.createVerticalStrut(10));

        JButton chooseFileButton = new JButton("Välj fil");
        chooseFileButton.setActionCommand("open");
        chooseFileButton.addActionListener(this);
        vbox.add(chooseFileButton);

        chosenFileField = new JTextField();
        chosenFileField.setEditable(false);
        vbox.add(chosenFileField);

        vbox.add(Box.createVerticalStrut(10));

        add(vbox);
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        setNextCard(Wizard.RUN);
        setState();
    }

    private void setState()
    {
        if (toSigelField.getText().matches("^[A-Za-z0-9]+$") && !fromSigelField.getText().isEmpty())
        {
            enableNext();
        }
        else
        {
            disableNext();
        }
    }

    @Override
    protected void beforeNext()
    {
        Set<String> ids = new HashSet<>();
        if (chosenFile != null)
        {
            try (BufferedReader reader = new BufferedReader(new FileReader(chosenFile)))
            {
                for (String line; (line = reader.readLine()) != null; )
                {
                    ids.add(line);
                }
            } catch (Throwable e) {
                Wizard.exitFatal(e);
            }
        }
        try
        {
            if (ids.isEmpty())
                setParameterForNextCard(ScriptGenerator.generateChangeSigelScript(fromSigelField.getText(), toSigelField.getText()));
            else
                setParameterForNextCard(ScriptGenerator.generateChangeSigelFromListScript(ids, fromSigelField.getText(), toSigelField.getText()));
        } catch (IOException ioe)
        {
            Wizard.exitFatal(ioe);
        }
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent)
    {
        if (actionEvent.getActionCommand().equals("open"))
        {
            int returnVal = chooser.showOpenDialog(window);
            if(returnVal == JFileChooser.APPROVE_OPTION)
            {
                chosenFile = chooser.getSelectedFile();
                chosenFileField.setText( chooser.getSelectedFile().getName() );
            }
        }
    }

    @Override
    public void insertUpdate(DocumentEvent documentEvent) { setState(); }

    @Override
    public void removeUpdate(DocumentEvent documentEvent) { setState(); }

    @Override
    public void changedUpdate(DocumentEvent documentEvent) { setState(); }
}

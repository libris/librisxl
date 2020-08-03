package whelk.gui;

import whelk.ScriptGenerator;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ChangeSigelPanel extends WizardCard implements DocumentListener
{
    private JTextField fromSigelField;
    private JTextField toSigelField;

    public ChangeSigelPanel(Wizard wizard)
    {
        super(wizard);

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
        if (toSigelField.getText().matches("^[A-Za-z]+$") && !fromSigelField.getText().isEmpty())
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
        try
        {
            setParameterForNextCard(ScriptGenerator.generateChangeSigelScript(fromSigelField.getText(), toSigelField.getText()));
        } catch (IOException ioe)
        {
            Wizard.exitFatal(ioe);
        }
    }

    @Override
    public void insertUpdate(DocumentEvent documentEvent) { setState(); }

    @Override
    public void removeUpdate(DocumentEvent documentEvent) { setState(); }

    @Override
    public void changedUpdate(DocumentEvent documentEvent) { setState(); }
}

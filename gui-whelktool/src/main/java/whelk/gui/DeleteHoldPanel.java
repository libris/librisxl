package whelk.gui;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import whelk.ScriptGenerator;

public class DeleteHoldPanel extends WizardCard implements ActionListener, DocumentListener
{
    final Wizard window;

    private JFileChooser chooser = new JFileChooser();
    private File chosenFile;
    private JTextField chosenFileField;
    private JTextField sigelField;

    public DeleteHoldPanel(Wizard wizard)
    {
        super(wizard);
        window = wizard;

        Box vbox = Box.createVerticalBox();

        vbox.add(new JLabel("<html>Vänligen välj en fil med IDn.<br/><br/>Filen måste innehålla libris bibliografiska kontrollnummer<br/>" +
                "motsvarande {@graph,0,controlNumber} eller MARC-fält 001.<br/><br/>T ex: 1890245 eller tb4s55f50hm8w1f.<br/>Ett ID per rad.</html>"));

        vbox.add(Box.createVerticalStrut(10));

        JButton chooseFileButton = new JButton("Välj fil");
        chooseFileButton.setActionCommand("open");
        chooseFileButton.addActionListener(this);
        vbox.add(chooseFileButton);

        vbox.add(Box.createVerticalStrut(10));

        chosenFileField = new JTextField();
        chosenFileField.setEditable(false);
        vbox.add(chosenFileField);

        vbox.add(Box.createVerticalStrut(10));

        vbox.add(new JLabel("Vänligen ange sigel för vilket bestånd ska tas bort."));
        sigelField = new JTextField();
        sigelField.getDocument().addDocumentListener(this);
        vbox.add(sigelField);

        this.add(vbox);
    }

    @Override
    protected void beforeNext()
    {
        Set<String> ids = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(chosenFile)))
        {
            for (String line; (line = reader.readLine()) != null; )
            {
                ids.add(line);
            }
        } catch (Throwable e) {
            Wizard.exitFatal(e);
        }
        try
        {
            setParameterForNextCard(ScriptGenerator.generateDeleteHoldScript(sigelField.getText(), ids));
        } catch (IOException ioe)
        {
            Wizard.exitFatal(ioe);
        }
    }

    private void setState()
    {
        if (chosenFile != null && sigelField.getText() != null && !sigelField.getText().equals(""))
        {
            enableNext();
        } else
        {
            disableNext();
        }
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        setNextCard(Wizard.RUN);
        setState();
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

        setState();
    }

    @Override
    public void insertUpdate(DocumentEvent documentEvent) { setState(); }

    @Override
    public void removeUpdate(DocumentEvent documentEvent) { setState(); }

    @Override
    public void changedUpdate(DocumentEvent documentEvent) { setState(); }
}

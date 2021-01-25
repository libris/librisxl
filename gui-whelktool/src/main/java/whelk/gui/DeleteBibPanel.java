package whelk.gui;

import whelk.ScriptGenerator;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

class DeleteBibPanel extends WizardCard implements ActionListener
{
    final Wizard window;

    private JFileChooser chooser = new JFileChooser();
    private File chosenFile;
    private JTextField chosenFileField;

    public DeleteBibPanel(Wizard wizard)
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

        add(vbox);
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
            setParameterForNextCard(ScriptGenerator.generateDeleteBibScript(ids));
        } catch (IOException ioe)
        {
            Wizard.exitFatal(ioe);
        }
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        setNextCard(Wizard.RUN);
        chosenFile = null;
        disableNext();
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
                enableNext();
            }
        }
    }
}
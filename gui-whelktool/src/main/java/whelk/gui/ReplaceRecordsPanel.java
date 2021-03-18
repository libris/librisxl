package whelk.gui;

import whelk.ScriptGenerator;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

class ReplaceRecordsPanel extends WizardCard implements ActionListener
{
    final Wizard window;

    private JFileChooser chooser = new JFileChooser();
    private File chosenFile;
    private JTextField chosenFileField;

    public ReplaceRecordsPanel(Wizard wizard)
    {
        super(wizard);
        window = wizard;

        chooser.setPreferredSize(new Dimension(1024, 768));

        Box vbox = Box.createVerticalBox();

        vbox.add(new JLabel("<html>Vänligen välj en fil med par av XL-IDn (EJ KONTROLLNUMMER!).<br/>" +
                "<br/>Filen måste innehålla två IDn per rad, separerade av ett mellanslag." +
                "<br/>Det första IDt på varje rad ersätter det andra IDt på samma rad." +
                "<br/><br/>Exempel:" +
                "<br/>vd6njp162pcr3zd c9ps03vw0cpqgx6" +
                "<br/>jvtbf1w0g7jhgttx fcrtxkcz4dxttr5" +
                "<br/><br/>Tolkas som:" +
                "<br/>c9ps03vw0cpqgx6 ersätts av vd6njp162pcr3zd." +
                "<br/>fcrtxkcz4dxttr5 ersätts av jvtbf1w0g7jhgttx." +
                "</html>"));

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
            setParameterForNextCard(ScriptGenerator.generateReplaceRecordsScript(ids));
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
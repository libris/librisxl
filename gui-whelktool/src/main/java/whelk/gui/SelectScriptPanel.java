package whelk.gui;

import whelk.PortableScript;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class SelectScriptPanel extends WizardCard implements ActionListener
{
    Wizard wizard;
    JLabel description = new JLabel();

    public SelectScriptPanel(Wizard wizard)
    {
        super(wizard);
        this.wizard = wizard;

        Box vBox = Box.createVerticalBox();

        JButton loadButton = new JButton("Öppna script-fil");
        loadButton.addActionListener(this);
        loadButton.setActionCommand("load");
        vBox.add(loadButton);

        vBox.add(Box.createVerticalStrut(10));
        vBox.add(new JLabel("Valt script:"));
        vBox.add(description);

        this.add(vBox);

    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        setNextCard(Wizard.RUN);
        disableNext();
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent)
    {
        if (actionEvent.getActionCommand().equals("load"))
        {
            JFileChooser chooser = new JFileChooser();
            chooser.setPreferredSize(new Dimension(1024, 768));
            int returnVal = chooser.showOpenDialog(wizard);
            if(returnVal == JFileChooser.APPROVE_OPTION)
            {
                File chosenFile = chooser.getSelectedFile();

                try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(chosenFile)))
                {
                    Object loaded = ois.readObject();
                    if (loaded instanceof PortableScript)
                    {
                        PortableScript loadedScript = (PortableScript) loaded;
                        description.setText(loadedScript.comment);
                        setParameterForNextCard(loaded);
                        enableNext();
                    }
                } catch (IOException | ClassNotFoundException ioe)
                {
                    Wizard.exitFatal(ioe.getMessage());
                }
            }
        }
    }
}

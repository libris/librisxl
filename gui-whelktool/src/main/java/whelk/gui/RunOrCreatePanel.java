package whelk.gui;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class RunOrCreatePanel extends WizardCard implements ActionListener
{
    private JRadioButton rCreate;
    private JRadioButton rRun;

    public RunOrCreatePanel(Wizard wizard)
    {
        super(wizard);

        this.setLayout(new GridBagLayout());

        boolean creationForbidden = System.getProperty("creationForbidden") != null &&
                System.getProperty("creationForbidden").equals("true");

        Box vbox = Box.createVerticalBox();
        if (creationForbidden)
        {
            rCreate = new JRadioButton("Skapa en körning (ej tillåtet i denna miljö).");
            rCreate.setEnabled(false);
        }
        else
        {
            rCreate = new JRadioButton("Skapa en körning.");
            rCreate.addActionListener(this);
            rCreate.setSelected(true);
        }
        rRun = new JRadioButton("Kör en tidigare skapad körning.");
        rRun.addActionListener(this);

        ButtonGroup group = new ButtonGroup();
        group.add(rCreate);
        group.add(rRun);

        vbox.add(rCreate);
        vbox.add(rRun);
        this.add(vbox);
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        chooseNextCard();
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent)
    {
        chooseNextCard();
    }

    private void chooseNextCard()
    {
        if (! rRun.isSelected())
            setNextCard(Wizard.CREATE_WHAT);
        else
            setNextCard(Wizard.SELECT_SCRIPT);
    }
}

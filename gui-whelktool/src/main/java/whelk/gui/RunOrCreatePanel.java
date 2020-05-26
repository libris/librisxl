package whelk.gui;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class RunOrCreatePanel extends WizardCard implements ActionListener
{
    private JRadioButton rCreate;
    private JRadioButton rRun;

    public RunOrCreatePanel(Wizard wizard)
    {
        super(wizard);

        Box vbox = Box.createVerticalBox();
        rCreate = new JRadioButton("Skapa en körning.");
        rCreate.addActionListener(this);
        rRun = new JRadioButton("Kör en tidigare skapad körning.");
        rRun.addActionListener(this);

        rCreate.setSelected(true);

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
        if (createSelected())
            setNextCard(Wizard.CREATE_WHAT);
        else
            setNextCard(Wizard.RUN);
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent)
    {
        if (createSelected())
            setNextCard(Wizard.CREATE_WHAT);
        else
            setNextCard(Wizard.RUN);
    }

    public boolean createSelected()
    {
        return ! rRun.isSelected();
    }
}

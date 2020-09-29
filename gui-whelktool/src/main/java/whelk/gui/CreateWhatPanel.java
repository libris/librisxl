package whelk.gui;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class CreateWhatPanel extends WizardCard implements ActionListener
{
    JRadioButton rDeleteHolds;
    JRadioButton rCreateHolds;
    JRadioButton rDeleteBibs;
    JRadioButton rChangeSigel;
    JRadioButton rReplaceRecords;

    public CreateWhatPanel(Wizard wizard)
    {
        super(wizard);

        this.setLayout(new GridBagLayout());

        Box vbox = Box.createVerticalBox();
        rDeleteHolds = new JRadioButton("Radera Bestånd.");
        rDeleteHolds.addActionListener(this);
        rCreateHolds = new JRadioButton("Skapa Bestånd.");
        rCreateHolds.addActionListener(this);
        rDeleteBibs = new JRadioButton("Radera bibliografiska poster utan bestånd.");
        rDeleteBibs.addActionListener(this);
        rChangeSigel = new JRadioButton("Byt en sigel mot en annan.");
        rChangeSigel.addActionListener(this);
        rReplaceRecords = new JRadioButton("Ersätt poster.");
        rReplaceRecords.addActionListener(this);

        rDeleteHolds.setSelected(true);

        ButtonGroup group = new ButtonGroup();
        group.add(rDeleteHolds);
        group.add(rCreateHolds);
        group.add(rDeleteBibs);
        group.add(rChangeSigel);
        group.add(rReplaceRecords);

        vbox.add(rDeleteHolds);
        vbox.add(rCreateHolds);
        vbox.add(rDeleteBibs);
        vbox.add(rChangeSigel);
        vbox.add(rReplaceRecords);
        this.add(vbox);
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        setNextCard(getSelectedAction());
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent) {
        setNextCard(getSelectedAction());
    }

    public String getSelectedAction()
    {
        if (rChangeSigel.isSelected())
            return Wizard.CHANGE_SIGEL;
        else if (rDeleteBibs.isSelected())
            return Wizard.DELETE_BIB;
        else if (rDeleteHolds.isSelected())
            return Wizard.DELETE_HOLD;
        else if (rCreateHolds.isSelected())
            return Wizard.CREATE_HOLD;
        else if (rReplaceRecords.isSelected())
            return Wizard.REPLACE_RECORDS;
        return null;
    }
}

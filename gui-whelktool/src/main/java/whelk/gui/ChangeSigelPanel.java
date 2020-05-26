package whelk.gui;

import javax.swing.*;

public class ChangeSigelPanel extends WizardCard
{
    public ChangeSigelPanel(Wizard wizard)
    {
        super(wizard);
        add(new JLabel("change sigel."));
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {

    }
}

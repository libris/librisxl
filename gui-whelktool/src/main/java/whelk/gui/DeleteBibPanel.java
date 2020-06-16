package whelk.gui;

import javax.swing.*;

class DeleteBibPanel extends WizardCard
{
    public DeleteBibPanel(Wizard wizard)
    {
        super(wizard);
        add(new JLabel("Delete bibs."));
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {

    }
}
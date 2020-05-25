package whelk.gui;

import javax.swing.*;

public class CreateWhatPanel extends WizardCard
{
    public CreateWhatPanel(Wizard wizard)
    {
        super(wizard);
        this.add(new JLabel(" OTHER TEXT OTHER TEXT "));
    }

    @Override
    void onShow()
    {
        setNextCard(Wizard.RUN_OR_CREATE);
    }
}

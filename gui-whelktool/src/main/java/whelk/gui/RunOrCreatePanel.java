package whelk.gui;

import javax.swing.*;

public class RunOrCreatePanel extends WizardCard
{
    public RunOrCreatePanel(Wizard wizard)
    {
        super(wizard);
        this.add(new JLabel(" TEXT TEXT TEXT "));

    }

    @Override
    void onShow()
    {
        setNextCard(Wizard.CREATE_WHAT);
    }
}

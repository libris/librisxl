package whelk.gui;

import javax.swing.*;

public abstract class WizardCard extends JPanel
{
    private Wizard wizard;
    public WizardCard(Wizard wizard)
    {
        this.wizard = wizard;
    }

    abstract void onShow(Object parameterFromPreviousCard);

    protected void beforeNext() {};

    protected void setNextCard(String card)
    {
        wizard.setNextCard(card);
    }

    protected void setParameterForNextCard(Object parameterFromPreviousCard)
    {
        wizard.setParameterForNextCard(parameterFromPreviousCard);
    }

    protected void enableNext()
    {
        wizard.enableNext();
    }

    protected void disableNext()
    {
        wizard.disableNext();
    }

    protected void enableBack()
    {
        wizard.enableBack();
    }

    protected void disableBack()
    {
        wizard.disableBack();
    }

    protected void enableCancel()
    {
        wizard.enableCancel();
    }

    protected void disableCancel()
    {
        wizard.disableCancel();
    }
}

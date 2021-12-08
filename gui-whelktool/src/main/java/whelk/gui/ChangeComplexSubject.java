package whelk.gui;

import whelk.ScriptGenerator;

import javax.swing.*;
import java.io.IOException;

public class ChangeComplexSubject extends WizardCard
{
    private final JTextField fromMainTerm;
    private final JTextField toMainTerm;

    public ChangeComplexSubject(Wizard wizard)
    {
        super(wizard);

        Box vbox = Box.createVerticalBox();

        vbox.add(new JLabel("Huvudord i sammansatt term att byta ut"));
        fromMainTerm = new JTextField();
        vbox.add(fromMainTerm);

        vbox.add(Box.createVerticalStrut(10));

        vbox.add(new JLabel("Nytt huvudord att byta till"));
        toMainTerm = new JTextField();
        vbox.add(toMainTerm);

        add(vbox);
    }

    @Override
    void onShow(Object parameterFromPreviousCard)
    {
        setNextCard(Wizard.RUN);
    }

    @Override
    protected void beforeNext()
    {
        try
        {
            setParameterForNextCard(ScriptGenerator.generateChangeSubjectScript(fromMainTerm.getText(), toMainTerm.getText()));
        } catch (IOException ioe)
        {
            Wizard.exitFatal(ioe);
        }
    }
}

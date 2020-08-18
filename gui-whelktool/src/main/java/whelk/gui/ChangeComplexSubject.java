package whelk.gui;

import whelk.ScriptGenerator;

import javax.swing.*;
import java.io.IOException;

public class ChangeComplexSubject extends WizardCard
{
    private JTextField fromSigelField;
    private JTextField toSigelField;

    public ChangeComplexSubject(Wizard wizard)
    {
        super(wizard);

        Box vbox = Box.createVerticalBox();

        vbox.add(new JLabel("Huvudord i sammansatt term att byta ut"));
        fromSigelField = new JTextField();
        vbox.add(fromSigelField);

        vbox.add(Box.createVerticalStrut(10));

        vbox.add(new JLabel("Nytt huvudord att byta till"));
        toSigelField = new JTextField();
        vbox.add(toSigelField);

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
            setParameterForNextCard(ScriptGenerator.generateChangeSubjectScript(fromSigelField.getText(), toSigelField.getText()));
        } catch (IOException ioe)
        {
            Wizard.exitFatal(ioe);
        }
    }
}

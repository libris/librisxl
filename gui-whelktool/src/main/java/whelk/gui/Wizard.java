package whelk.gui;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;
import java.util.Stack;

class Wizard extends JDialog implements ActionListener
{
    public static String EXIT = "exit";
    public static String BACK = "back";
    public static String RUN_OR_CREATE = "runorcreate";
    public static String CREATE_WHAT = "createwhat";
    public static String RUN = "runexisting";
    public static String DELETE_BIB = "deletebibs";
    public static String DELETE_HOLD = "deleteholds";
    public static String CREATE_HOLD = "createholds";
    public static String CHANGE_SIGEL = "changesigel";
    public static String SELECT_SCRIPT = "selectscript";

    private JPanel cardPanel;
    private JButton backButton;
    private JButton nextButton;
    private JButton cancelButton;
    private CardLayout cardLayout;

    private Stack<String> cardStack;
    private String currentCard;
    private HashMap<String, WizardCard> cardInstances;

    private Object parameterFromPreviousCard;

    public Wizard()
    {
        cardStack = new Stack<>();
        cardInstances = new HashMap<>();

        cardInstances.put(RUN_OR_CREATE, new RunOrCreatePanel(this));
        cardInstances.put(CREATE_WHAT, new CreateWhatPanel(this));
        cardInstances.put(RUN, new RunPanel(this));
        cardInstances.put(DELETE_BIB, new DeleteBibPanel(this));
        cardInstances.put(DELETE_HOLD, new DeleteOrCreateHoldPanel(this, false));
        cardInstances.put(CREATE_HOLD, new DeleteOrCreateHoldPanel(this, true));
        cardInstances.put(CHANGE_SIGEL, new ChangeSigelPanel(this));
        cardInstances.put(SELECT_SCRIPT, new SelectScriptPanel(this));

        JPanel buttonPanel = new JPanel();
        Box buttonBox = new Box(BoxLayout.X_AXIS);

        cardPanel = new JPanel();
        cardPanel.setBorder(new EmptyBorder(new Insets(5, 10, 5, 10)));

        cardLayout = new CardLayout();
        cardPanel.setLayout(cardLayout);
        backButton = new JButton("Tillbaka");
        backButton.addActionListener(this);
        backButton.setActionCommand(BACK);
        backButton.setEnabled(false);
        nextButton = new JButton("Nästa");
        nextButton.addActionListener(this);
        cancelButton = new JButton("Avsluta");
        cancelButton.addActionListener(this);
        cancelButton.setActionCommand(EXIT);

        for (String key : cardInstances.keySet())
        {
            cardPanel.add(cardInstances.get(key), key);
        }

        buttonPanel.setLayout(new BorderLayout());
        buttonPanel.add(new JSeparator(), BorderLayout.NORTH);

        buttonBox.setBorder(new EmptyBorder(new Insets(5, 10, 5, 10)));
        buttonBox.add(backButton);
        buttonBox.add(Box.createHorizontalStrut(10));
        buttonBox.add(nextButton);
        buttonBox.add(Box.createHorizontalStrut(30));
        buttonBox.add(cancelButton);
        buttonPanel.add(buttonBox, java.awt.BorderLayout.EAST);
        this.getContentPane().add(buttonPanel, java.awt.BorderLayout.SOUTH);
        this.getContentPane().add(cardPanel, java.awt.BorderLayout.CENTER);

        currentCard = RUN_OR_CREATE;
        cardInstances.get(RUN_OR_CREATE).onShow(null);
        cardLayout.show(cardPanel, RUN_OR_CREATE);

        this.setTitle("LibrisXL globala ändringar");
        this.setResizable(false);

        this.pack();
    }

    public void setNextCard(String card)
    {
        nextButton.setActionCommand(card);
    }

    public void setParameterForNextCard(Object parameterFromPreviousCard)
    {
        this.parameterFromPreviousCard = parameterFromPreviousCard;
    }

    public void enableNext()
    {
        nextButton.setEnabled(true);
    }

    public void disableNext()
    {
        nextButton.setEnabled(false);
    }

    public void enableBack()
    {
        backButton.setEnabled(true);
    }

    public void disableBack()
    {
        backButton.setEnabled(false);
    }

    public void enableCancel()
    {
        cancelButton.setEnabled(true);
    }

    public void disableCancel()
    {
        cancelButton.setEnabled(false);
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent) {
        String actionCommand = actionEvent.getActionCommand();

        if (actionCommand.equals(EXIT))
        {
            this.dispose();
            return;
        }

        CardLayout layout = (CardLayout)(cardPanel.getLayout());

        if (actionCommand.equals(BACK))
        {
            String toCard = cardStack.pop();
            currentCard = toCard;
            nextButton.setActionCommand("");
            parameterFromPreviousCard = null;
            if (cardStack.isEmpty()) disableBack();
            else enableBack();
            enableNext();
            enableCancel();
            cardInstances.get(toCard).onShow(null);
            layout.show(cardPanel, toCard);
        } else {
            if (actionCommand.equals(currentCard))
                return;

            cardInstances.get(currentCard).beforeNext();

            cardStack.push(currentCard);
            currentCard = actionCommand;
            nextButton.setActionCommand("");
            enableBack();
            enableNext();
            enableCancel();
            cardInstances.get(actionCommand).onShow(parameterFromPreviousCard);
            layout.show(cardPanel, actionCommand);
        }
    }

    static void exitFatal(Throwable e)
    {
        System.out.println(e.toString());
        e.printStackTrace();
        JOptionPane.showMessageDialog(null, e.getMessage(), "Message", JOptionPane.INFORMATION_MESSAGE);
        System.exit(-1);
    }

    static void exitFatal(String message)
    {
        System.out.println(message);
        JOptionPane.showMessageDialog(null, message, "Message", JOptionPane.INFORMATION_MESSAGE);
        System.exit(-1);
    }
}
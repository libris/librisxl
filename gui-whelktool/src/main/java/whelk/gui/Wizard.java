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
    public static String BACK = "back";
    public static String RUN_OR_CREATE = "runorcreate";
    public static String CREATE_WHAT = "createwhat";

    private JPanel cardPanel;
    private JButton backButton;
    private JButton nextButton;
    private JButton cancelButton;
    private CardLayout cardLayout;

    private Stack<String> cardStack;
    private String currentCard;
    private HashMap<String, WizardCard> cardInstances;

    public Wizard()
    {
        cardStack = new Stack<>();
        cardInstances = new HashMap<>();

        cardInstances.put(RUN_OR_CREATE, new RunOrCreatePanel(this));
        cardInstances.put(CREATE_WHAT, new CreateWhatPanel(this));

        JPanel buttonPanel = new JPanel();
        Box buttonBox = new Box(BoxLayout.X_AXIS);

        cardPanel = new JPanel();
        cardPanel.setBorder(new EmptyBorder(new Insets(5, 10, 5, 10)));

        cardLayout = new CardLayout();
        cardPanel.setLayout(cardLayout);
        backButton = new JButton("Back");
        backButton.addActionListener(this);
        backButton.setActionCommand(BACK);
        backButton.setEnabled(false);
        nextButton = new JButton("Next");
        nextButton.addActionListener(this);
        cancelButton = new JButton("Cancel");

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
        cardInstances.get(RUN_OR_CREATE).onShow();

        this.pack();
    }

    public void setNextCard(String card)
    {
        nextButton.setActionCommand(card);
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent) {
        String actionCommand = actionEvent.getActionCommand();
        CardLayout layout = (CardLayout)(cardPanel.getLayout());

        if (actionCommand.equals(BACK))
        {
            String toCard = cardStack.pop();
            currentCard = toCard;
            cardInstances.get(toCard).onShow();
            layout.show(cardPanel, toCard);
            if (cardStack.isEmpty())
            {
                backButton.setEnabled(false);
            }
        } else {
            if (actionCommand.equals(currentCard))
                return;

            cardStack.push(currentCard);
            currentCard = actionCommand;
            cardInstances.get(actionCommand).onShow();
            layout.show(cardPanel, actionCommand);
            backButton.setEnabled(true);
        }
    }
}
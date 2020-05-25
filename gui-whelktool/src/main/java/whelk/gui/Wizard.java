package whelk.gui;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

class Wizard extends JDialog implements ActionListener
{
    public static String RUN_OR_CREATE = "runorcreate";
    public static String CREATE_WHAT = "createwhat";

    JPanel cardPanel;
    JButton backButton;
    JButton nextButton;
    JButton cancelButton;
    CardLayout cardLayout;


    public Wizard()
    {
        JPanel buttonPanel = new JPanel();
        Box buttonBox = new Box(BoxLayout.X_AXIS);

        cardPanel = new JPanel();
        cardPanel.setBorder(new EmptyBorder(new Insets(5, 10, 5, 10)));

        cardLayout = new CardLayout();
        cardPanel.setLayout(cardLayout);
        backButton = new JButton("Back");
        backButton.addActionListener(this);
        nextButton = new JButton("Next");
        nextButton.addActionListener(this);
        nextButton.setActionCommand(CREATE_WHAT); // TEMP
        cancelButton = new JButton("Cancel");

        cardPanel.add(new RunOrCreatePanel(), RUN_OR_CREATE);
        cardPanel.add(new CreateWhatPanel(), CREATE_WHAT);

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
        this.pack();
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent) {
        CardLayout layout = (CardLayout)(cardPanel.getLayout());
        layout.show(cardPanel, actionEvent.getActionCommand());
    }
}
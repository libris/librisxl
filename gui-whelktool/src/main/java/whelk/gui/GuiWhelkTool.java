package whelk.gui;

import javax.swing.*;

class GuiWhelkTool
{
    public static void main(String args[])
    {
        java.awt.EventQueue.invokeLater(new Runnable()
        {
            public void run()
            {
                Wizard wizard = new Wizard();
                wizard.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
                wizard.setVisible(true);
            }
        });
    }
}

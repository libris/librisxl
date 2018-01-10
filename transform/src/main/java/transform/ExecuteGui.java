package transform;

import javax.swing.*;
import java.awt.*;

public class ExecuteGui extends JFrame
{
    public ExecuteGui()
    {
        this.setTitle("Libris XL data transform");
        this.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        BoxLayout boxLayout = new BoxLayout(this.getContentPane(), BoxLayout.Y_AXIS);
        this.getContentPane().setLayout(boxLayout);

        JButton b1 = new JButton("Load script");
        this.getContentPane().add( makeLeftAligned(b1) );

        JComponent jc = makeLeftAligned(getTextArea("SELECT * from lddb", 4, 40, false, true));
        this.getContentPane().add( makeLeftAligned(new JLabel("Run on:")) );
        this.getContentPane().add( jc );

        JComponent scriptArea = makeLeftAligned(getTextArea("# SCRIPT GOES HERE", 20, 40, true, true));
        JComponent editPanel = makeLeftAligned(new JPanel());
        editPanel.setLayout(new BorderLayout());
        editPanel.add( makeLeftAligned(new JLabel("Transformation Script:")), BorderLayout.NORTH );
        editPanel.add( scriptArea, BorderLayout.CENTER );
        this.getContentPane().add( editPanel );

        JPanel jsonDisplay = new JPanel();
        jsonDisplay.setLayout(new GridLayout(1, 2));

        JPanel buttonPanel = new JPanel();
        this.getContentPane().add(makeLeftAligned(buttonPanel));
        buttonPanel.add(new JButton("Next"));
        buttonPanel.add(new JButton("Run"));

        JPanel before = new JPanel();
        before.setLayout(new BorderLayout(10, 10));
        before.add( new JLabel("Before execution:"), BorderLayout.NORTH );
        before.add( getTextArea("", 20, 40, true, false), BorderLayout.CENTER );

        JPanel after = new JPanel();
        after.setLayout(new BorderLayout(10, 10));
        after.add( new JLabel("After execution:"), BorderLayout.NORTH );
        after.add( getTextArea("", 20, 40, true, false), BorderLayout.CENTER );

        jsonDisplay.add( makeLeftAligned(before), 0 );
        jsonDisplay.add( makeLeftAligned(after), 1 );

        this.getContentPane().add(makeLeftAligned(jsonDisplay));

        this.pack();
        this.setVisible(true);
    }

    private JComponent getTextArea(String s, int lines, int columns, boolean scrolls, boolean editable)
    {
        JTextArea area = new JTextArea(s, lines, columns);
        area.setLineWrap(true);
        area.setEditable(editable);
        if (scrolls)
        {
            JScrollPane sp = new JScrollPane(area);
            return sp;
        }
        return area;
    }

    private JComponent makeLeftAligned(JComponent c)
    {
        c.setAlignmentX(Component.LEFT_ALIGNMENT);
        return c;
    }
}

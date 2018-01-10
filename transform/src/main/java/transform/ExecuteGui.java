package transform;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.*;
import java.util.Properties;

public class ExecuteGui extends JFrame
{
    public ExecuteGui()
    {
        ActionResponse actionResponse = new ActionResponse(this);

        this.setTitle("Libris XL data transform");
        this.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        BoxLayout boxLayout = new BoxLayout(this.getContentPane(), BoxLayout.Y_AXIS);
        this.getContentPane().setLayout(boxLayout);

        JMenuBar menuBar = new JMenuBar();
        JMenu fileMenu = new JMenu("File");
        menuBar.add(fileMenu);
        fileMenu.setMnemonic(KeyEvent.VK_F);
        this.setJMenuBar(menuBar);

        JMenuItem loadItem = new JMenuItem("Open");
        loadItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.ALT_MASK));
        loadItem.addActionListener(actionResponse);
        fileMenu.add(loadItem);
        JMenuItem saveItem = new JMenuItem("Save");
        saveItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, ActionEvent.ALT_MASK));
        saveItem.addActionListener(actionResponse);
        fileMenu.add(saveItem);
        JMenuItem saveAsItem = new JMenuItem("SaveAs");
        saveAsItem.addActionListener(actionResponse);
        fileMenu.add(saveAsItem);
        JMenuItem loadEnvironment = new JMenuItem("LoadEnvironmentFile");
        loadEnvironment.addActionListener(actionResponse);
        fileMenu.add(loadEnvironment);

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

    private class ActionResponse implements ActionListener
    {
        private Component m_parent;
        private JFileChooser m_fileChooser = new JFileChooser();
        private File m_currentFile = null;
        private Properties m_envProps = null;

        public ActionResponse(Component parent)
        {
            m_parent = parent;
        }

        @Override
        public void actionPerformed(ActionEvent ae)
        {
            int result;

            switch(ae.getActionCommand())
            {
                case "Open":
                    result = m_fileChooser.showOpenDialog(m_parent);
                    if (result == JFileChooser.APPROVE_OPTION)
                    {
                        m_currentFile = m_fileChooser.getSelectedFile();
                        load();
                    }
                    break;
                case "Save":
                    if (m_currentFile == null)
                    {
                        result = m_fileChooser.showOpenDialog(m_parent);
                        if (result == JFileChooser.APPROVE_OPTION)
                        {
                            m_currentFile = m_fileChooser.getSelectedFile();
                        }
                    }
                    if (m_currentFile != null)
                        save();
                    break;
                case "SaveAs":
                    result = m_fileChooser.showOpenDialog(m_parent);
                    if (result == JFileChooser.APPROVE_OPTION)
                    {
                        m_currentFile = m_fileChooser.getSelectedFile();
                        save();
                    }
                    break;
                case "LoadEnvironmentFile":
                    result = m_fileChooser.showOpenDialog(m_parent);
                    if (result == JFileChooser.APPROVE_OPTION)
                    {
                        File envFile = m_fileChooser.getSelectedFile();
                        System.out.println("Load env-file from: " + envFile);
                        try
                        {
                            InputStream propStream = new FileInputStream(envFile);
                            m_envProps.load(propStream);
                        } catch (IOException e){/* ignore */}
                    }
                    break;
            }

        }

        private void load()
        {
            try
            {
                BufferedReader reader = new BufferedReader(new FileReader(m_currentFile));
                StringBuilder sb = new StringBuilder();
                String line;
                while ( (line = reader.readLine()) != null)
                {
                    sb.append(line);
                    sb.append("\n");
                }
                System.out.println(sb.toString());
            } catch (IOException ioe)
            {
                JOptionPane.showMessageDialog(m_parent, ioe.toString());
            }

            System.out.println("Load from: " + m_currentFile);
        }

        private void save()
        {
            System.out.println("Save to: " + m_currentFile);
        }
    }
}

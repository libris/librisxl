package transform;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.triples.JsonldSerializer;
import whelk.util.PropertyLoader;
import whelk.util.TransformScript;
import whelk.util.URIWrapper;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class ExecuteGui extends JFrame
{
    public JTextArea m_scriptTextArea;
    public JTextArea m_sqlTextArea;
    public JTextArea m_originalRecordArea;
    public JTextArea m_transformedRecordArea;

    public ExecuteGui()
    {
        ActionResponse actionResponse = new ActionResponse(this);

        this.setLocationRelativeTo(null);
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

        m_sqlTextArea = getTextArea("SELECT id FROM lddb WHERE collection <> 'definitions' and deleted = false", 4, 40, true);
        JComponent jc = makeLeftAligned(m_sqlTextArea);
        JComponent selectPanel = makeLeftAligned(new JPanel());
        selectPanel.setLayout(new BorderLayout());
        selectPanel.add( makeLeftAligned(new JLabel("Select (short) IDs to operate on:")), BorderLayout.NORTH );
        selectPanel.add( jc, BorderLayout.CENTER );

        m_scriptTextArea = getTextArea("# All key words are case-insensitive. Any symbol that contains whitespace must be\n" +
                "# surrounded by double quotes.\n" +
                "# The #-character considers the rest of the line a comment and ignores it.\n" +
                "# Every script must begin with either \"mode normal\" or \"mode framed\"\n" +
                "# If framed mode is used, data will be framed before the script is applied.\n" +
                "# Framed or not, data is always returned to Libris-normal form after application of the script.\n" +
                "#\n" +
                "# MOVE [path1] -> [path2]\n" +
                "# Moves a part of the data structure from path1 to path2 (creating path2 if necessary)\n" +
                "# example:\n" +
                "#   move @graph,0,created -> @graph,0,minted\n" +
                "#\n" +
                "# SET [value] -> [path]\n" +
                "# Set a literal value at a specific path (creating the path if necessary)\n" +
                "# example:\n" +
                "#   set http://libris.kb.se/library/S -> @graph,1,heldBy,@id\n" +
                "#\n" +
                "# DELETE [path]\n" +
                "# Deletes whatever is at path\n" +
                "# example:\n" +
                "#   delete @graph,0,modified\n" +
                "#\n" +
                "# FOREACH [iterator-symbol] : [path]\n" +
                "# Run the subsequent block of code, once for each member of the list at path, which\n" +
                "# must point to a list. The elements are always traversed in descending order, to avoid\n" +
                "# the iterator invalidation problem in case of removals.\n" +
                "# example:\n" +
                "#   foreach it : @graph {\n" +
                "#     set \"ok\" -> @graph,it,someKey\n" +
                "#   }\n" +
                "\nmode normal\n", 35, 40, true);
        JComponent scriptArea = makeLeftAligned(new JScrollPane(m_scriptTextArea));
        JComponent editPanel = makeLeftAligned(new JPanel());
        editPanel.setLayout(new BorderLayout());
        editPanel.add( makeLeftAligned(new JLabel("Transformation Script:")), BorderLayout.NORTH );
        editPanel.add( scriptArea, BorderLayout.CENTER );

        JSplitPane split1 = new JSplitPane(JSplitPane.VERTICAL_SPLIT, selectPanel, editPanel);
        this.getContentPane().add( split1 );

        JPanel jsonDisplay = new JPanel();
        jsonDisplay.setLayout(new GridLayout(1, 2));

        JPanel buttonPanel = new JPanel();
        JButton b0 = new JButton("Try again (without saving)");
        b0.setActionCommand("TryAgain");
        b0.addActionListener(actionResponse);
        JButton b1 = new JButton("Try next (without saving)");
        b1.setActionCommand("Try");
        b1.addActionListener(actionResponse);
        JButton b2 = new JButton("Execute and save all records");
        b2.setActionCommand("ExecuteAll");
        b2.addActionListener(actionResponse);
        JButton b3 = new JButton("Reset");
        b3.setActionCommand("Reset");
        b3.addActionListener(actionResponse);
        buttonPanel.add(b0);
        buttonPanel.add(b1);
        buttonPanel.add(b3);
        buttonPanel.add(b2);

        JPanel before = new JPanel();
        before.setLayout(new BorderLayout(10, 10));
        before.add( new JLabel("Before execution:"), BorderLayout.NORTH );
        m_originalRecordArea = getTextArea("", 20, 40, false);
        before.add( new JScrollPane(m_originalRecordArea), BorderLayout.CENTER );

        JPanel after = new JPanel();
        after.setLayout(new BorderLayout(10, 10));
        after.add( new JLabel("After execution:"), BorderLayout.NORTH );
        m_transformedRecordArea = getTextArea("", 20, 40, false);
        after.add( new JScrollPane(m_transformedRecordArea), BorderLayout.CENTER );

        JPanel resultPanel = (JPanel) makeLeftAligned(new JPanel());
        resultPanel.setLayout(new BorderLayout());
        resultPanel.add(makeLeftAligned(buttonPanel), BorderLayout.NORTH);

        jsonDisplay.add( makeLeftAligned(before), 0 );
        jsonDisplay.add( makeLeftAligned(after), 1 );

        resultPanel.add(makeLeftAligned(jsonDisplay), BorderLayout.CENTER);

        JSplitPane split2 = new JSplitPane(JSplitPane.VERTICAL_SPLIT, split1, resultPanel);
        this.getContentPane().add(split2);

        this.pack();
        this.setVisible(true);
    }

    private JTextArea getTextArea(String s, int lines, int columns, boolean editable)
    {
        JTextArea area = new JTextArea(s, lines, columns);
        area.setLineWrap(true);
        area.setEditable(editable);
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
        private JLabel m_progressLabel = null;
        private Whelk m_whelk = null;
        private ObjectMapper m_mapper = new ObjectMapper();

        private Connection m_connection;
        private PreparedStatement m_statement;
        private ResultSet m_resultSet;
        private Document m_lastDocument;

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
                        executeUnderDialog("Connecting", "Connecting to XL environment, please wait..", () ->
                            {
                                try
                                {
                                    javax.swing.SwingUtilities.invokeLater( () -> m_progressLabel.setText("Loading config..") );
                                    File envFile = m_fileChooser.getSelectedFile();
                                    InputStream propStream = new FileInputStream(envFile);
                                    m_envProps = new Properties();
                                    m_envProps.load(propStream);
                                    javax.swing.SwingUtilities.invokeLater( () -> m_progressLabel.setText("Starting Whelk..") );
                                    m_whelk = new Whelk(m_envProps);
                                    Document.setBASE_URI( new URIWrapper( (String) m_envProps.get("baseUri")) );
                                    m_whelk.loadCoreData();
                                } catch (IOException ioe)
                                {
                                    JOptionPane.showMessageDialog(m_parent, ioe.toString());
                                }
                            });
                    }
                    break;
                case "Try":
                    if (m_whelk != null)
                    {
                        if (m_resultSet == null)
                        {
                            startNewTrySeries();
                        }
                        showNextInTrySeries();
                    }
                    break;
                case "TryAgain":
                    if (m_lastDocument != null)
                    {
                        showTransformation(m_lastDocument);
                    }
                    break;
                case "Reset":
                    resetTrySeries();
                    break;
                case "ExecuteAll":
                    String confirmation = JOptionPane.showInputDialog(m_parent,
                            "Please understand that performing this operation will permanently alter data in Libris.\n" +
                            "To show that you understand this and want to proceed, please answer \"DESTROY DATA\".");
                    if (confirmation.equals("DESTROY DATA"))
                        executeUnderDialog("Running transformation", "Running.", this::executeAll);
                    else
                        JOptionPane.showMessageDialog(m_parent, "You did not correctly type \"DESTROY DATA\", so no changes were made.");
                    break;
            }

        }

        private void executeAll()
        {
            ExecuteGui parent = (ExecuteGui) m_parent;
            String sqlString = parent.m_sqlTextArea.getText();
            if (isObviouslyBadSql(sqlString)) {
                JOptionPane.showMessageDialog(m_parent, "Denied: Suspicious SQL statement.");
                return;
            }

            try
            {
                final TransformScript script = new TransformScript(parent.m_scriptTextArea.getText());

                Date now = new Date();

                try(Connection connection = m_whelk.getStorage().getConnection();
                PreparedStatement statement = connection.prepareStatement(sqlString);
                ResultSet resultSet = statement.executeQuery();
                PrintWriter successWriter = new PrintWriter("transformations_ok" + now.toString() + ".log");
                PrintWriter failureWriter = new PrintWriter("transformations_failed" + now.toString() + ".log"))
                {
                    while (resultSet.next())
                    {
                        String shortId = resultSet.getString(1);
                        System.out.println("Will now do: " + shortId);
                        m_whelk.storeAtomicUpdate(shortId, false, "xl", "Libris admin", (Document doc) ->
                        {
                            try
                            {
                                doc.data = script.executeOn(doc.data);
                                doc.data = JsonLd.flatten(doc.data);
                                JsonldSerializer.normalize(doc.data, doc.getCompleteId(), false);
                            } catch (Throwable e)
                            {
                                failureWriter.println(shortId);
                            }
                        });

                        successWriter.println(shortId);
                    }
                } catch (SQLException e)
                {
                    JOptionPane.showMessageDialog(m_parent, "SQL failure: " + e);
                }
            } catch (TransformScript.TransformSyntaxException | FileNotFoundException e)
            {
                JOptionPane.showMessageDialog(m_parent, e.toString());
            }
        }

        private void startNewTrySeries()
        {
            ExecuteGui parent = (ExecuteGui) m_parent;
            String sqlString = parent.m_sqlTextArea.getText();
            if (isObviouslyBadSql(sqlString)) {
                JOptionPane.showMessageDialog(m_parent, "Denied: Suspicious SQL statement.");
                return;
            }

            try
            {
                m_connection = m_whelk.getStorage().getConnection();
                m_statement = m_connection.prepareStatement(sqlString);
                m_resultSet = m_statement.executeQuery();
            } catch (Exception e) {
                resetTrySeries();
                JOptionPane.showMessageDialog(m_parent, e.toString());
            }
        }

        private void showNextInTrySeries()
        {
            try
            {
                if (m_resultSet.next())
                {
                    String shortId = m_resultSet.getString(1);
                    Document document = m_whelk.getStorage().load(shortId);
                    m_lastDocument = document.clone();
                    showTransformation(document);
                }
                else
                {
                    JOptionPane.showMessageDialog(m_parent, "All changes reviewed.");
                    resetTrySeries();
                }
            } catch (SQLException e)
            {
                JOptionPane.showMessageDialog(m_parent, e.toString());
            }
        }

        private void showTransformation(Document _document)
        {
            Document document = _document.clone();
            try {
                String formattedOriginal = m_mapper.writerWithDefaultPrettyPrinter().writeValueAsString(document.data);
                ExecuteGui parent = (ExecuteGui) m_parent;
                parent.m_originalRecordArea.setText(formattedOriginal);
                TransformScript script = new TransformScript(parent.m_scriptTextArea.getText());
                Map transformedData = script.executeOn(document.data);
                transformedData = JsonLd.flatten(transformedData);
                JsonldSerializer.normalize(transformedData, document.getCompleteId(), false);
                String formattedTransformed = m_mapper.writerWithDefaultPrettyPrinter().writeValueAsString(transformedData);
                parent.m_transformedRecordArea.setText(formattedTransformed);
            } catch (Throwable e)
            {
                JOptionPane.showMessageDialog(m_parent, e.toString());
            }
        }

        private void resetTrySeries()
        {
            try { if (m_resultSet != null) m_resultSet.close(); } catch (SQLException e) { /* ignore */ }
            try { if (m_statement != null) m_statement.close(); } catch (SQLException e) { /* ignore */ }
            try { if (m_connection != null) m_connection.close(); } catch (SQLException e) { /* ignore */ }
            m_resultSet = null;
            m_statement = null;
            m_connection = null;
        }

        private boolean isObviouslyBadSql(String sql)
        {
            String[] badWords =
                    {
                            "DROP",
                            "TRUNCATE",
                            "MODIFY",
                            "ALTER",
                            "UPDATE",
                    };

            for (String word : badWords)
                if (StringUtils.containsIgnoreCase(sql, word))
                    return true;
            return false;
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
                ((ExecuteGui) m_parent).m_scriptTextArea.setText(sb.toString());
                reader.close();
            } catch (IOException ioe)
            {
                JOptionPane.showMessageDialog(m_parent, ioe.toString());
            }

        }

        private void save()
        {
            try
            {
                PrintWriter writer = new PrintWriter(m_currentFile);
                String text = ((ExecuteGui) m_parent).m_scriptTextArea.getText();
                writer.write(text);
                writer.close();
            } catch (IOException ioe)
            {
                JOptionPane.showMessageDialog(m_parent, ioe.toString());
            }
        }

        private void executeUnderDialog(String title, String description, Runnable runnable)
        {
            JDialog dialog = new JDialog( (JFrame) m_parent, title, true );
            dialog.setSize(280, 80);
            dialog.getContentPane().setLayout(new BoxLayout( dialog.getContentPane(), BoxLayout.Y_AXIS ));
            m_progressLabel = new JLabel(description);
            dialog.add(m_progressLabel);
            JProgressBar pb = new JProgressBar();
            pb.setIndeterminate(true);
            dialog.add(pb);
            dialog.setLocationRelativeTo(m_parent);
            dialog.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);

            // The horror, FU swing.
            Thread worker = new Thread(runnable);
            worker.start();
            new Thread( () ->
            {
                while (true)
                {
                    if (worker.getState() == Thread.State.TERMINATED)
                    {
                        dialog.setVisible(false);
                        return;
                    }
                }
            }).start();
            new Thread( () -> dialog.setVisible(true) ).start();
        }
    }
}

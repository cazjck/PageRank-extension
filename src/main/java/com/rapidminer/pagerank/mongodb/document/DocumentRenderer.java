package com.rapidminer.pagerank.mongodb.document;

import com.rapidminer.gui.renderer.*;
import com.rapidminer.operator.*;
import com.rapidminer.report.*;
import com.rapidminer.tools.*;
import javax.swing.table.*;
import java.awt.*;
import com.rapidminer.gui.tools.*;
import javax.swing.text.*;
import javax.swing.*;
/**
 * 
 * @author Text extension of RapidMiner
 * https://marketplace.rapidminer.com/UpdateServer/faces/product_details.xhtml?productId=rmx_text
 *
 */
public class DocumentRenderer extends AbstractRenderer
{
    public Reportable createReportable(final Object renderable, final IOContainer ioContainer, final int desiredWidth, final int desiredHeight) {
        return null;
    }
    
    public String getName() {
        return "Document";
    }
    
    public Component getVisualizationComponent(final Object renderable, final IOContainer ioContainer) {
        final Document textObject = (Document)renderable;
        final JPanel result = new JPanel(new BorderLayout());
        final JTable table = (JTable)new ExtendedJTable((TableModel)new AbstractTableModel() {
            private static final long serialVersionUID = 800642825392116250L;
            
            @Override
            public String getColumnName(final int index) {
                if (index == 0) {
                    return "Property";
                }
                return "Value";
            }
            
            @Override
            public int getColumnCount() {
                return 2;
            }
            
            @Override
            public int getRowCount() {
                return textObject.getMetaDataKeys().size();
            }
            
            @Override
            public Object getValueAt(final int rowIndex, final int columnIndex) {
                final String key = textObject.getMetaDataKeys().toArray(new String[0])[rowIndex];
                if (key == null) {
                    return "?";
                }
                if (columnIndex != 0) {
                    return Tools.format(textObject.getMetaDataValue(key), textObject.getMetaDataType(key));
                }
                if (key.startsWith("metadata_")) {
                    return key.substring("metadata_".length());
                }
                return key;
            }
        }, false);
        table.setTableHeader(null);
        table.setBorder(BorderFactory.createMatteBorder(0, 1, 0, 0, Color.LIGHT_GRAY));
        result.add(table, "East");
        final JPanel texts = new JPanel(new GridLayout(2, 1));
        JTextPane textArea = new JTextPane();
        textArea.setEditable(false);
        final SimpleAttributeSet attributeSet = new SimpleAttributeSet();
        StyledDocument doc = textArea.getStyledDocument();
        boolean flag = true;
        for (final Token s : textObject.getTokenSequence()) {
            if (flag) {
                StyleConstants.setForeground(attributeSet, new Color(255, 51, 204));
            }
            else {
                StyleConstants.setForeground(attributeSet, new Color(51, 51, 255));
            }
            flag = !flag;
            try {
                doc.insertString(doc.getLength(), s.getToken() + " ", attributeSet);
            }
            catch (BadLocationException ex) {}
        }
        textArea.setCaretPosition(0);
        JScrollPane pane = (JScrollPane)new ExtendedJScrollPane((Component)textArea);
        pane.setBorder(BorderFactory.createMatteBorder(0, 0, 1, 0, Color.LIGHT_GRAY));
        texts.add(pane);
        textArea = new JTextPane();
        textArea.setEditable(false);
        doc = textArea.getStyledDocument();
        try {
            doc.insertString(0, textObject.getDisplayText(), new SimpleAttributeSet());
        }
        catch (BadLocationException ex2) {}
        textArea.setCaretPosition(0);
        pane = (JScrollPane)new ExtendedJScrollPane((Component)textArea);
        pane.setBorder(null);
        texts.add(pane);
        result.add(texts, "Center");
        return result;
    }
}


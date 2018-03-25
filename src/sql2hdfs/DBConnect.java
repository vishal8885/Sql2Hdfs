/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sql2hdfs;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author tarun
 */
public class DBConnect {

    private Property p;
    private Connection con;
    private PreparedStatement st;

    public DBConnect(Property p) {
        this.p = p;
        try {
            Class.forName(p.getDriver());
            con = DriverManager.getConnection(p.getUrl(), p.getUser(), p.getPass());
        } catch (Exception e) {
            JOptionPane.showMessageDialog(null, e);
        }
    }

    public Connection getConnection() {
        return con;
    }

    public String[] getTables() {
        List<String> tables = new ArrayList<String>();
        tables.add("select");
        try {
            DatabaseMetaData md = con.getMetaData();
            ResultSet rs = md.getTables(null, null, "%", null);
            while (rs.next()) {
                tables.add(rs.getString(3));
            }
        } catch (SQLException ex) {
            Logger.getLogger(DBConnect.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tables.toArray(new String[0]);
    }

    public void setTableData(String query, JTable jt,JTextArea ta) {
        List<String> columns = new ArrayList<String>();
        try {
            if(!query.contains("SELECT")){
                query = "SELECT * FROM "+query;
            }
            st = con.prepareStatement(query);
            ResultSet rs = st.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            String row[] = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                columns.add(rsmd.getColumnName(i));
            }
            DefaultTableModel dtm = new DefaultTableModel(columns.toArray(new String[0]), 0);
            jt.setModel(dtm);
            int j = 0;
            while (rs.next()) {
                j=0;
                for (int i = 1; i <= columnCount; i++) {
                    row[j++] = rs.getString(i);
                }
                dtm.addRow(row);
            }
            ta.setText(query);

        } catch (SQLException ex) {
            if(!ex.getMessage().contains("right syntax to use near 'select' at line 1"))
                JOptionPane.showMessageDialog(null, ex.getMessage());
        }
        return;
    }

    public boolean checkConnection() {
        if (con == null) {
            return false;
        }
        return true;
    }
}

package com.visDis.recommendation;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author ninja
 */
public class PoolsBuilder {

    private String tableName;
    private ArrayList<String> dimensionPool;
    private ArrayList<String> measurePool;
    private ArrayList<String> specificAttPool;

    private SqLiteHelper sql;
    private ResultSet rs;

    public PoolsBuilder(String tableName) throws ClassNotFoundException, SQLException {

        this.tableName = tableName;
        this.dimensionPool = new ArrayList<String>();
        this.measurePool = new ArrayList<String>();
        this.specificAttPool = new ArrayList<String>();

        this.sql = new SqLiteHelper();

        this.setPools();
    }

    public String getTableName() {
        return this.tableName;
    }

    public ArrayList<String> getDimensionPool() {
        return dimensionPool;
    }

    public ArrayList<String> getMeasurePool() {
        return measurePool;
    }

    public ArrayList<String> getSpecificAttPool() {
        return specificAttPool;
    }

    private String concat(String... s) {
        StringBuilder b = new StringBuilder();
        for (String x : s) {
            b.append(x);
        }
        return b.toString();
    }

    public void setPools() throws SQLException {

        rs = sql.executeQuery(concat("SELECT * FROM ", this.tableName));
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            rs = sql.executeQuery(concat("SELECT ", rsmd.getColumnName(i), " FROM ", this.tableName, " WHERE 1"));

            if (rs.getString(1).matches("-?\\d+(\\.\\d+)?") == true && rsmd.getColumnName(i).toLowerCase().equals("year") == false && rsmd.getColumnName(i).equals("years") == false && rsmd.getColumnName(i).equals("id") == false) {
                this.measurePool.add(rsmd.getColumnName(i));
            } else {
                rs = sql.executeQuery(concat("SELECT COUNT(DISTINCT `", rsmd.getColumnName(i), "`) AS NUM_of_values FROM '", this.tableName, "'"));

                // System.out.println(rs.getString(1) + rsmd.getColumnName(i));
                if (Integer.parseInt(rs.getString(1)) == 2) {
                    this.specificAttPool.add(rsmd.getColumnName(i));
                } else if (Integer.parseInt(rs.getString(1)) < 15 && Integer.parseInt(rs.getString(1)) > 2) {

                    this.dimensionPool.add(rsmd.getColumnName(i));
                }
            }

        }

    }

}

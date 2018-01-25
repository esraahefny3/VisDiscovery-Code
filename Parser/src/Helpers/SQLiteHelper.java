/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Helpers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import Helpers.Constants;
import Helpers.ParseCSV;
import Helpers.Strings;
import java.sql.*;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Nada Essam
 */
public class SQLiteHelper {

    public static Connection con;
    public static Statement stmt;

    /*public SQLiteHelper(String tableName, HashMap<String, ArrayList<String>> table) throws ClassNotFoundException, SQLException {

     this.con = null;
     this.stmt = null;
     this.getConnection();
     //this.CreateTable(tableName, table);
     //this.InsertIntoTable(tableName, table);
     }*/
    public SQLiteHelper(String tableName) throws ClassNotFoundException, SQLException {

        this.con = null;
        this.stmt = null;
        this.getConnection();
        //this.CreateTable(tableName, table);
        //this.InsertIntoTable(tableName, table);
    }

    public static void getConnection() {

        try {
            Class.forName("org.sqlite.JDBC"); //dynamically loaded
            String DBpath = "jdbc:sqlite:Yello_Taxi.db"; // Load an SQLite DB
            con = DriverManager.getConnection(DBpath); // create a connection to the database
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Connection to SQLite has been established.");

    }

    /* public void CreateTable(String tableName, HashMap<String, ArrayList<String>> table) {

     try {
     stmt = con.createStatement();
     String CreateTableSQL = "CREATE TABLE " + Constants.DEFAULT_CSV + " ( id INTEGER PRIMARY KEY ";
     for (String key : table.keySet()) {
     CreateTableSQL = CreateTableSQL + ", " + key + "  " + "TEXT";
     }
     CreateTableSQL = CreateTableSQL + " )";
     System.out.println(CreateTableSQL);
     stmt.executeUpdate(CreateTableSQL);
     stmt.close();
     System.out.println("Table created successfully");
     } catch (SQLException ex) {
     Logger.getLogger(SQLiteHelper.class.getName()).log(Level.SEVERE, null, ex);
     }

     }

     public void InsertIntoTable(String tableName, HashMap<String, ArrayList<String>> table) {
     //stmt = con.createStatement();
     for (int i = 0; i < table.size(); i++) {
     String InsertionSQL = "INSERT INTO " + Constants.DEFAULT_CSV + " ( id ";
     for (String key : table.keySet()) {
     InsertionSQL = InsertionSQL + ", " + key;
     }
     InsertionSQL = InsertionSQL + " )" + "VALUES (" + i;
     for (Map.Entry<String, ArrayList<String>> entry : table.entrySet()) {
     ArrayList<String> List = entry.getValue();
     for (int j = 0; j < List.size(); j++) {
     InsertionSQL = InsertionSQL + List.get(j);
     }
     }
     System.out.println(InsertionSQL);
     }
     }*/
    public void CreateTable(String tableName, String[] keys) {

        try {
            stmt = con.createStatement();
            String DROPTABLESQL = "DROP TABLE IF EXISTS " + Constants.DEFAULT_CSV ;
            stmt.executeUpdate(DROPTABLESQL);
            String CreateTableSQL = "CREATE TABLE IF NOT EXISTS " + Constants.DEFAULT_CSV + " ( id INTEGER PRIMARY KEY AUTOINCREMENT ";
            for (int i = 0; i < keys.length; i++) {
                CreateTableSQL = CreateTableSQL + ", " + keys[i] + "  " + "TEXT NULL";
            }
            CreateTableSQL = CreateTableSQL + " )";
            //System.out.println(CreateTableSQL);
            stmt.executeUpdate(CreateTableSQL);
            stmt.close();
            System.out.println("Table created successfully");
        } catch (SQLException ex) {
            Logger.getLogger(SQLiteHelper.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public void InsertIntoTable(String tableName, String key[], String value[], int iteration) {
        try {
            stmt = con.createStatement();
            String InsertionSQL = "INSERT INTO " + Constants.DEFAULT_CSV + " ( ";
            for (int j = 0; j < key.length; j++) {
                if(j==0)
                    InsertionSQL = InsertionSQL + key[j];
                else
                    InsertionSQL = InsertionSQL + ", " + key[j];
            }
            InsertionSQL = InsertionSQL + " )" + " VALUES ( '";
            for (int k = 0; k < key.length; k++) {
                if(k < value.length ) {
                    if(k==0)
                        InsertionSQL = InsertionSQL + value[k].replace("'", "\\") + "'";
                    else
                        InsertionSQL = InsertionSQL + ", '" + value[k].replace("'", "\\") + "'";
                } else {
                    if(k==0)
                        InsertionSQL = InsertionSQL + null + "'";
                    else
                        InsertionSQL = InsertionSQL + ", '" + null + "'";
                }
            }
            InsertionSQL = InsertionSQL + " ); ";
            System.out.println(InsertionSQL);
            stmt.executeUpdate(InsertionSQL);
        } catch (Exception ex) {
            Logger.getLogger(SQLiteHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Helpers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author omarkrostom
 */
public class ParseCSV extends HelperMethods {

    public String filename;
    public String row;
    public HashMap table;
    public String[] keys;
    public int iteration;
    public SQLiteHelper DB;
    String[] dataExtracted;

    public ParseCSV(String filename) throws ClassNotFoundException, SQLException {
        DB = new SQLiteHelper(this.filename);
        this.table = new HashMap<String, ArrayList<String>>();
        this.filename = filename;
        try {
            this.initiateParsing(new BufferedReader(new FileReader(Constants.csvParseLocation + this.filename)), Constants.csvParseSeparator);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ParseCSV.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public ParseCSV(String filename, String regex) throws ClassNotFoundException, SQLException {
        try {
            this.initiateParsing(new BufferedReader(new FileReader(Constants.csvParseLocation + this.filename)), regex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ParseCSV.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void initiateParsing(BufferedReader br, String regex) {

        iteration = 0;
        if (checkDirectory(this.filename)) {
            try {
                while (((row = br.readLine()) != null)) {
                    dataExtracted = row.split(regex);
                    if (iteration == 0) {
                        this.keys = new String[dataExtracted.length];
                        for (int i = 0; i < this.keys.length; i++) {
                            keys[i] = dataExtracted[i];
                            if (keys[i].contains(" ")) {
                                keys[i] = this.keys[i].replaceAll(" ", "_");

                            }
                            ArrayList<String> temp = new ArrayList<>();
                            table.put(keys[i], temp);
                        }
                        DB.CreateTable(filename, keys);
                    } else {
                        DB.InsertIntoTable(filename, keys, dataExtracted, iteration);
                    }
                    System.out.println(iteration);
                    iteration++;

                }
                System.out.println("Insertion Completed");

            } catch (IOException ex) {
                Logger.getLogger(ParseCSV.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void viewColoumn(String coloumn_name, boolean hasBrackets) {
        if (hasBrackets) {
            if (checkArray("\"" + coloumn_name + "\"")) {
                System.out.println(table.get("\"" + coloumn_name + "\""));
            } else {
                System.out.println(Strings.no_coloumn_name);
            }
        } else {
            if (checkArray(coloumn_name)) {
                System.out.println(table.get(coloumn_name));
            } else {
                System.out.println(Strings.no_coloumn_name);
            }
        }
    }

    public boolean checkArray(String coloumn_name) {
        for (int i = 0; i < this.keys.length; i++) {
            if (keys[i] == coloumn_name) {
                return true;
            }
        }
        return false;
    }

    public void ModifyKeyName() {
        for (int i = 0; i < this.keys.length; i++) {
            if (this.keys[i].contains(" ")) {
                this.keys[i] = this.keys[i].replaceAll(" ", "_");

            }
        }
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Helpers;

/**
 *
 * @author omarkrostom
 */
public class Constants {
    public static String csvParseLocation = System.getProperty("user.dir") + "/resources/csv/";
    public static String csvParseSeparator = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    public static String DEFAULT_CSV_FILE = "Yellow_Taxi.csv";
    public static String DEFAULT_CSV = "Yellow_Taxi";
}

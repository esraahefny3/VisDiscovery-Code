/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Helpers;

import java.io.File;

/**
 *
 * @author omarkrostom
 */
public class HelperMethods {
        
    public boolean checkDirectory(String filename) {
        File filecheck = new File(Constants.csvParseLocation + filename);
        boolean check = (filecheck.isFile()) ? true : false;
        return check;
    }

}

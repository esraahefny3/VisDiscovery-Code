package com.visDis.recommendation;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import main.JDBCclass;

/**
 *
 * @author ninja
 */
public class VisExtract {

    private String tableName;
    private String x_axis;
    private String y_axis;
    private String aggregateFunction;
    private String selectorOnDimension;
    private String SpecificAttribute;
    private String firstOperator;
    private String firstSelector;
    private String secondOperator;
    private String secondSelector;

    private ArrayList<VisData> recommendationList;
    private ArrayList<VisData> mainVisList;
    private VisData vd;
    private CustomWrapper cw;

    private JDBCclass sql;
    private ResultSet rs;

    public VisExtract(String tableName, String xAxis, String yAxis, String aggregate, String selectorOnDimension, String attribute, String firstOperator, String firstSelector, String secondOperator, String secondSelector) throws ClassNotFoundException, SQLException {

        this.tableName = tableName;
        this.x_axis = xAxis;//D
        this.y_axis = yAxis;//M
        this.aggregateFunction = aggregate;//F
        this.selectorOnDimension = selectorOnDimension;
        this.SpecificAttribute = attribute;
        this.firstOperator = firstOperator;
        this.firstSelector = firstSelector;
        this.secondOperator = secondOperator;
        this.secondSelector = secondSelector;

        this.sql = new JDBCclass("jdbc", "192.168.56.2", "21050");
        this.recommendationList = new ArrayList<VisData>();
        this.mainVisList = new ArrayList<VisData>();

    }

    private String concat(String... s) {
        StringBuilder b = new StringBuilder();
        for (String x : s) {
            b.append(x);
        }
        return b.toString();
    }

    public void getMainVis() {
        /* 
            Expected Distribution: //Run the query on the entire data - not spacifying any selection
                                     this is is case the user specify only one selector
                    
            Actual Distribution://Run the same query on the subset of data we are interested in
                                  this os case he give only one selector 
         */

        vd = new VisData();
        vd.x_axis = this.x_axis;
        vd.y_axis = this.aggregateFunction + "[" + this.y_axis + "]";

        /*
            "SELECT x-axis, aggregate(y-axis),CASE WHEN Specific-attribute '=' first-selector THEN 1 WHEN Specific-attribute '=' second-selector THEN 2 END as g1 
             FROM table-name WHERE y-axis '=' selector-on-dimension GROUP BY x-axis, g1"
         */
        if (this.selectorOnDimension.equals("")) {
            PreparedStatement prepStmt = this.sql.getPreparedStmtForCustomQuery(concat("SELECT ", this.x_axis, ",", this.aggregateFunction, "(", this.y_axis, ")",
                    ",", "CASE WHEN ", this.SpecificAttribute, this.firstOperator, this.firstSelector, " THEN 1",
                    " WHEN ", this.SpecificAttribute, this.secondOperator, this.secondSelector, " THEN 2 END as g1",
                    " FROM ", this.tableName,
                    " GROUP BY ", this.x_axis, ",", "g1"));
            try {
                rs = prepStmt.executeQuery();

            } catch (SQLException ex) {
                Logger.getLogger(VisExtract.class.getName()).log(Level.SEVERE, null, ex);
            }

        } else {

            PreparedStatement prepStmt = this.sql.getPreparedStmtForCustomQuery(concat("SELECT ", this.x_axis, ",", this.aggregateFunction, "(", this.y_axis, ")",
                    "," + "CASE WHEN ", this.SpecificAttribute + this.firstOperator + this.firstSelector, " THEN 1",
                    " WHEN ", this.SpecificAttribute, this.secondOperator, this.secondSelector, " THEN 2 END as g1",
                    " FROM ", this.tableName,
                    " WHERE ", this.y_axis, "=", this.selectorOnDimension,
                    " GROUP BY ", this.x_axis, ",", "g1"));
            try {

                rs = prepStmt.executeQuery();

            } catch (SQLException ex) {
                Logger.getLogger(VisExtract.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        try {

            while (rs.next()) {

                if (rs.getString(3) != null) {

                    cw = new CustomWrapper();
                    cw.setFirstEntry(rs.getString(1));
                    cw.setSecondEntry(rs.getString(2));
                    vd.combinedQuery.put(cw, rs.getString(3));
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(VisExtract.class.getName()).log(Level.SEVERE, null, ex);
        }

        this.mainVisList.add(vd);
    }

    //Try all:  D * F * M
    public void getRecommendations() {

        for (int i = 0; i < VisBuilder.dimentionAttributeArray.length; i++) {

            for (int j = 0; j < VisBuilder.aggregateArray.length; j++) {

                for (int k = 0; k < VisBuilder.MeasureAttributeArray.length; k++) {

                    vd = new VisData();
                    vd.x_axis = VisBuilder.dimentionAttributeArray[i];
                    vd.y_axis = VisBuilder.aggregateArray[j] + "[" + VisBuilder.MeasureAttributeArray[k] + "]";
                    PreparedStatement prepStmt = this.sql.getPreparedStmtForCustomQuery(concat("SELECT ", VisBuilder.dimentionAttributeArray[i], ",", VisBuilder.aggregateArray[j], "(", VisBuilder.MeasureAttributeArray[k], ")",
                            "," + "CASE WHEN ", this.SpecificAttribute + this.firstOperator, this.firstSelector, " THEN 1",
                            " WHEN ", this.SpecificAttribute, this.secondOperator, this.secondSelector, " THEN 2 END as g1",
                            " FROM ", this.tableName,
                            " GROUP BY ", VisBuilder.dimentionAttributeArray[i], ",", "g1"));
                    try {
                        rs = prepStmt.executeQuery();
                    } catch (SQLException ex) {
                        Logger.getLogger(VisExtract.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    try {

                        while (rs.next()) {

                            if (rs.getString(3) != null) {

                                cw = new CustomWrapper();
                                cw.setFirstEntry(rs.getString(1));
                                cw.setSecondEntry(rs.getString(2));
                                vd.combinedQuery.put(cw, rs.getString(3));

                                if (rs.getString(3).equals("1")) {
                                    vd.recomQuery1.put(rs.getString(1), rs.getString(2));
                                } else if (rs.getString(3).equals("3")) {
                                    vd.recomQuery2.put(rs.getString(1), rs.getString(2));
                                }
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(VisExtract.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    this.recommendationList.add(vd);
                }
            }
        }

        euclidean_metric();
        //manhattan_metric();
        rank();
    }

    //update the total_devation entry in each object
    private void euclidean_metric() {
        /*  
                     Euclidean Distance
            d(p,q)=sqrt( (p1-q1)^2 + (p2-q2)^2 + (p2-q2)^2 + .... )
         
         */

        ArrayList arrayOfValues = new ArrayList();
        String pKey, qValueString;
        double pValue, qValue;

        for (int i = 0; i < this.recommendationList.size(); i++) {

            for (Map.Entry iterator : this.recommendationList.get(i).recomQuery2.entrySet()) {

                pKey = (String) iterator.getKey();
                pValue = Double.valueOf((String) iterator.getValue());

                qValueString = this.recommendationList.get(i).recomQuery1.get(pKey);

                if (qValueString == null) {
                    qValue = 0;
                } else {
                    qValue = Double.valueOf(qValueString);
                }

                this.recommendationList.get(i).totalDeviation += Math.pow(pValue - qValue, 2);
                arrayOfValues.add(pKey);
            }

            for (Map.Entry iterator : this.recommendationList.get(i).recomQuery1.entrySet()) {
                if (arrayOfValues.contains((String) iterator.getKey())) {
                    continue;
                }
                this.recommendationList.get(i).totalDeviation += Math.pow(Double.valueOf((String) iterator.getValue()), 2);
            }

            this.recommendationList.get(i).totalDeviation = Math.sqrt(this.recommendationList.get(i).totalDeviation);
        }
    }

    private void manhattan_metric() {
        /*  
                     manhattan Distance 
            d(p,q)=abs (p1-q1) + abs(p2-q2) + abs(p3-q3) 
         
         */

        ArrayList arrayOfValues = new ArrayList();
        String pKey, qValueString;
        double pValue, qValue;

        for (int i = 0; i < this.recommendationList.size(); i++) {

            for (Map.Entry iterator : this.recommendationList.get(i).recomQuery2.entrySet()) {

                pKey = (String) iterator.getKey();
                pValue = Double.valueOf((String) iterator.getValue());

                qValueString = this.recommendationList.get(i).recomQuery1.get(pKey);

                if (qValueString == null) {
                    qValue = 0;
                } else {
                    qValue = Double.valueOf(qValueString);
                }

                this.recommendationList.get(i).totalDeviation += Math.abs(qValue - pValue);
                arrayOfValues.add(pKey);
            }

            for (Map.Entry iterator : this.recommendationList.get(i).recomQuery1.entrySet()) {
                if (arrayOfValues.contains((String) iterator.getKey())) {
                    continue;
                }
                this.recommendationList.get(i).totalDeviation += Math.abs(Double.valueOf((String) iterator.getValue()));
            }

        }
    }

    //rank visualizations according to Deviations 
    private void rank() {
        Collections.sort(this.recommendationList);
    }

    //Show visualizations
    public void showVis() {

        /*
            two for loop here is not nessesory this is for the sake of printing in particular format 
            as in real visualization we will ckeck on the third value in the for loop over the hashMap
         */
        System.out.println();
        System.out.println("***************My main visualization**************** ");
        System.out.println();

        for (int j = 0; j < this.mainVisList.size(); j++) {

            cw = new CustomWrapper();

            System.out.println("First Query: ");
            System.out.println();

            for (Map.Entry iterator : this.mainVisList.get(j).combinedQuery.entrySet()) {
                if (((String) iterator.getValue()).equals("1")) {
                    cw = (CustomWrapper) iterator.getKey();

                    System.out.println("**X-axis** ( " + mainVisList.get(j).x_axis + " ): " + cw.getFirstEntry() + " **Y-axis** ( " + mainVisList.get(j).y_axis + " ): " + cw.getSecondEntry());
                }
            }

            System.out.println();
            System.out.println("Second Query: ");
            System.out.println();

            for (Map.Entry iterator : this.mainVisList.get(j).combinedQuery.entrySet()) {
                if (((String) iterator.getValue()).equals("2")) {
                    cw = (CustomWrapper) iterator.getKey();

                    System.out.println("**X-axis** ( " + mainVisList.get(j).x_axis + " ): " + cw.getFirstEntry() + " **Y-axis** ( " + mainVisList.get(j).y_axis + " ): " + cw.getSecondEntry());
                }
            }
        }

        System.out.println();
        System.out.println("***************Recommendations**************** ");
        System.out.println();

        for (int i = 0; i < this.recommendationList.size(); i++) {

            cw = new CustomWrapper();

            System.out.println();
            System.out.println("recommendation (" + (i + 1) + ")");
            System.out.println();

            System.out.println("Deviation: " + this.recommendationList.get(i).totalDeviation);
            System.out.println();

            System.out.println("First query: ");
            System.out.println();
            for (Map.Entry iterator : this.recommendationList.get(i).combinedQuery.entrySet()) {
                if (((String) iterator.getValue()).equals("1")) {
                    cw = (CustomWrapper) iterator.getKey();

                    System.out.println("**X-axis** ( " + recommendationList.get(i).x_axis + " ): " + cw.getFirstEntry() + " **Y-axis** ( " + recommendationList.get(i).y_axis + " ): " + cw.getSecondEntry());
                }
            }

            System.out.println();
            System.out.println("Second query: ");
            System.out.println();

            for (Map.Entry iterator : this.recommendationList.get(i).combinedQuery.entrySet()) {
                if (((String) iterator.getValue()).equals("2")) {
                    cw = (CustomWrapper) iterator.getKey();

                    System.out.println("**X-axis** ( " + recommendationList.get(i).x_axis + " ): " + cw.getFirstEntry() + " **Y-axis** ( " + recommendationList.get(i).y_axis + " ): " + cw.getSecondEntry());
                }
            }
        }

    }

}

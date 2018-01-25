package com.visDis.recommendation;

/**
 *
 * @author ninja
 */
public class VisBuilder {

    /*we nedd to know: columns name , dimenstions/measures ??*/
   
    /*
    Table columns in Impala:
    
    Accident, Belts, Personal_Injury, State(MD/DC/VA/TX), Year, Make(HONDA/TOYOTA/...), 
    Color, Race, Gender, Driver_City, Driver_State, Charge  
    
     */
    private String tableName = "traffic_violation_parq";
    private String x_axis_D = "Race";//Dimention Attribute (D)
    private String y_axis_M = "Accident";//Measure Attribute (M) 
    private String aggregate_F = "Count";//Aggregate function (F)

    /*
      If the user wants to aggregate on a dimension he must put another selector on that dimension. 
      Here he want to know to races who done most of the accidents, so we count Accident(but accident is dimension) 
      so we need another selector on the diemnsion which is accident=yes
     */
    private String selectorOnDimension = "'Yes'";

    /* The subset of the data where we will run the 2 queries on it and the recommendations will change accroding to them*/
    private String SpecificAttribute = "Gender";
    private String firstOperator = "=";
    private String firstSelector = "'M'";
    private String secondOperator = "=";
    private String secondSelector = "'F'";

    public static int numOfVis = 5;//number or recommendations that will be shown to the user


    /* Pool of (F), (D), (M) according to the table*/
    public static String[] aggregateArray = {"COUNT", "AVG"};
    public static String[] dimentionAttributeArray = {"State", "Year", "Make", "Race", "Color", "Driver_City", "Driver_State", "Belts", "Accident"};
    public static String[] MeasureAttributeArray = {"Cast(Charge AS INT)"};

    public String tableName() {
        return this.tableName;
    }

    public String getXAxis() {
        return this.x_axis_D;
    }

    public String getYAxis() {
        return this.y_axis_M;
    }

    public String getAggregate() {
        return this.aggregate_F;
    }

    public String getSelectorOnDimension() {
        return this.selectorOnDimension;
    }

    public String getSpecificAttribute() {
        return this.SpecificAttribute;
    }

    public String getFirstOperator() {
        return this.firstOperator;
    }

    public String getfirstSelector() {
        return this.firstSelector;
    }

    public String getSecondOperator() {
        return this.secondOperator;
    }

    public String getSecondSelector() {
        return this.secondSelector;
    }

}

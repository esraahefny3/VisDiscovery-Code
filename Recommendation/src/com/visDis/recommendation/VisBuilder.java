package com.visDis.recommendation;

/**
 *
 * @author ninja
 */
public class VisBuilder {

    
    /*
    Table columns in our DataBase here <Parser.db>:
    
    Accident, Belts, Personal_Injury, State(MD/DC/VA/TX), Year, Make(HONDA/TOYOTA/...), 
    Color, Race, Gender, Driver_City, Driver_State, Charge  
    
     */
    private String tableName = "Wuzzuf";
    private String x_axis_D = "career_level";//Dimention Attribute (D)
    private String y_axis_M = "salary_minimum";//Measure Attribute (M) 
    private String aggregate_F = "SUM";//Aggregate function (F)

    /*
      If the user wants to aggregate on a dimension he must put another selector on that dimension. 
      Here he want to know to races who done most of the accidents, so we count Accident(but accident is dimension) 
      so we need another selector on the diemnsion which is accident=yes
     */
    private String selectorOnDimension = "";

    /* The subset of the data where we will run the 2 queries on it and the recommendations will change accroding to them*/
    private String SpecificAttribute = "city_name";
    private String firstOperator = "=";
    private String firstSelector = "'alexandria'";
    private String secondOperator = "=";
    private String secondSelector = "'cairo'";

    public static int numOfVis = 3;//number or recommendations that will be shown to the user


    /* Pool of (F), (D), (M) according to the table*/
    public static String[] aggregateArray = {"AVG"};
    public static String[] dimentionAttributeArray = {"career_level","currency","job_categories"};
    public static String[] MeasureAttributeArray = {"salary_minimum","salary_maximum","num_vacancies","views","applicants_count"};

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setXAxis(String xAxis) {
        this.x_axis_D = xAxis;
    }

    public void setYAxis(String yAxis) {
        this.y_axis_M = yAxis;
    }

    public void setAggregateFunction(String aggregateFunction) {
        this.aggregate_F = aggregateFunction;
    }

    public void setSelectorOnDimension(String SOD) {
        this.selectorOnDimension = SOD;
    }

    public void setSpecificAttribute(String specificAttribute) {
        this.SpecificAttribute = specificAttribute;
    }

    public void setFirstOperator(String firstOperator) {
        this.firstOperator = firstOperator;
    }

    public void setfirstSelector(String firstSelector) {
        this.firstSelector = firstSelector;
    }

    public void setSecondOperator(String secondOperator) {
        this.secondOperator = secondOperator;
    }

    public void setSecondSelector(String secondSelector) {
        this.secondSelector = secondSelector;
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getXAxis() {
        return this.x_axis_D;
    }

    public String getYAxis() {
        return this.y_axis_M;
    }

    public String getAggregateFunction() {
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

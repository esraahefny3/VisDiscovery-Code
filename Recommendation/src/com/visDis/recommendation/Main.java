package com.visDis.recommendation;

import java.sql.SQLException;

/**
 *
 * @author ninja
 */
public class Main {

    
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
      
       
        VisBuilder vb = new VisBuilder();
        VisExtract extract = new VisExtract(vb.getTableName(), vb.getXAxis(), vb.getYAxis(),
                vb.getAggregateFunction(), vb.getSelectorOnDimension(),
                vb.getSpecificAttribute(), vb.getFirstOperator(), vb.getfirstSelector(),
                vb.getSecondOperator(), vb.getSecondSelector()
        );

        extract.getMainVis();

        extract.getRecommendations_bruteForce();
        extract.showVis();
        
        //extract.getRecommendation_optimized();
       // extract.showVis2();
      
    }
    
}

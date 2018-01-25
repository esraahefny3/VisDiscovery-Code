package com.visDis.recommendation;

import java.sql.SQLException;

/**
 *
 * @author ninja
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // TODO code application logic here

        VisBuilder vb = new VisBuilder();
        VisExtract extract = new VisExtract(vb.tableName(), vb.getXAxis(), vb.getYAxis(),
                vb.getAggregate(), vb.getSelectorOnDimension(),
                vb.getSpecificAttribute(), vb.getFirstOperator(), vb.getfirstSelector(),
                vb.getSecondOperator(), vb.getSecondSelector()
        );

        extract.getMainVis();

        extract.getRecommendations();

        extract.showVis();
    }

}

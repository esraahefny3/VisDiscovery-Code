package com.visDis.recommendation;

import java.util.HashMap;

/**
 *
 * @author ninja
 */
public class VisData implements Comparable<VisData> {

    public double totalDeviation = 0;
    public String visType;
    public String x_axis;
    public String y_axis;
    public HashMap<String, String> recomQuery1 = new HashMap<String, String>();
    public HashMap<String, String> recomQuery2 = new HashMap<String, String>();

    public HashMap<CustomWrapper, String> combinedQuery = new HashMap<CustomWrapper, String>();

    @Override
    public int compareTo(VisData t) {
        return ((int) t.totalDeviation - (int) totalDeviation);
    }
}

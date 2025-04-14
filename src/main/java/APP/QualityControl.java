package APP;

import com.alibaba.fastjson.JSONObject;
import java.util.List;

public class QualityControl {
    private static final String CHL_FIELD = "chl";
    private static final String CDOM_FIELD = "cdom";
    private static final String TURBIDITY_FIELD = "turbidity";
    
    private static final Double chl_SmallSize = 50.0;
    private static final Double chl_BigSize = 4130.0;
    private static final Double fdom_SmallSize = 50.0;
    private static final Double fdom_BigSize = 4130.0;
    private static final Double ntu_SmallSiz = 50.0;
    private static final Double ntu_BigSize = 4130.0;

    public static void performInstrumentTest(JSONObject value) {
        JSONObject jsdata = value.getJSONObject("data");
        JSONObject qc_info = new JSONObject();
        value.put("qc_info", qc_info);

        Double chl_count = jsdata.getDouble("chl_count");
        Double turbidity_count = jsdata.getDouble("turbidity_count");
        Double cdom_count = jsdata.getDouble("cdom_count");

        if (chl_count > chl_BigSize || chl_count < chl_SmallSize) {
            qc_info.put(CHL_FIELD, "4,110");
        }
        if (turbidity_count > ntu_BigSize || turbidity_count < ntu_SmallSiz) {
            qc_info.put(TURBIDITY_FIELD, "4,110");
        }
        if (cdom_count > fdom_BigSize || cdom_count < fdom_SmallSize) {
            qc_info.put(CDOM_FIELD, "4,110");
        }
    }

    public static void performJianFengTest(List<JSONObject> jf_CountList, String field_name, boolean firstWindow) {
        if (jf_CountList.size() < 5) {
            return;
        }

        double avg = calculateAverage(jf_CountList, field_name);

        if (firstWindow) {
            calculateAndMark(jf_CountList, field_name, avg, 0);
            calculateAndMark(jf_CountList, field_name, avg, 1);
        }

        calculateAndMark(jf_CountList, field_name, avg, 2);
    }

    private static double calculateAverage(List<JSONObject> window, String field_name) {
        return (window.get(0).getJSONObject("data").getDouble(field_name) +
                window.get(1).getJSONObject("data").getDouble(field_name) +
                window.get(2).getJSONObject("data").getDouble(field_name) +
                window.get(3).getJSONObject("data").getDouble(field_name) +
                window.get(4).getJSONObject("data").getDouble(field_name)) / 5.0;
    }

    private static void calculateAndMark(List<JSONObject> window, String field_name, double avg, int index) {
        JSONObject qc_info = window.get(index).getJSONObject("qc_info");
        if (qc_info.containsKey(field_name)) {
            return;
        }

        double data = window.get(index).getJSONObject("data").getDouble(field_name);
        double threshold = 0.3 * data;
        boolean isAnomaly = data - avg >= threshold;

        if (isAnomaly) {
            qc_info.put(field_name, "3,1110");
        }
    }
} 
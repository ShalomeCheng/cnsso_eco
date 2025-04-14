package APP;

import com.alibaba.fastjson.JSONObject;
import java.util.List;

public class QualityControl {
    /*
     * 仪器测试
     */
    public static void performInstrumentTest(JSONObject value) {
        JSONObject jsdata = value.getJSONObject(Constants.DATA);
        JSONObject qc_info = new JSONObject();
        value.put(Constants.QC_INFO, qc_info);

        Double chl_count = jsdata.getDouble("chl_count");
        Double turbidity_count = jsdata.getDouble("turbidity_count");
        Double cdom_count = jsdata.getDouble("cdom_count");

        if (chl_count > Constants.CHL_BIG_SIZE || chl_count < Constants.CHL_SMALL_SIZE) {
            qc_info.put(Constants.CHL_FIELD, Constants.INSTRUMENT_FAILURE_MARK);
        }
        if (turbidity_count > Constants.NTU_BIG_SIZE || turbidity_count < Constants.NTU_SMALL_SIZE) {
            qc_info.put(Constants.TURBIDITY_FIELD, Constants.INSTRUMENT_FAILURE_MARK);
        }
        if (cdom_count > Constants.FDOM_BIG_SIZE || cdom_count < Constants.FDOM_SMALL_SIZE) {
            qc_info.put(Constants.CDOM_FIELD, Constants.INSTRUMENT_FAILURE_MARK);
        }
    }

    /*
     * 尖峰测试
     */
    public static void performJianFengTest(List<JSONObject> jf_CountList, String field_name, boolean firstWindow) {
        if (jf_CountList.size() < Constants.JIANFENG_WINDOW_SIZE) {
            return;
        }

        double avg = calculateAverage(jf_CountList, field_name);

        if (firstWindow) {
            calculateAndMark(jf_CountList, field_name, avg, 0);
            calculateAndMark(jf_CountList, field_name, avg, 1);
        }

        calculateAndMark(jf_CountList, field_name, avg, 2);
    }

    /*
     * 计算平均值
     */
    private static double calculateAverage(List<JSONObject> window, String field_name) {
        return (window.get(0).getJSONObject(Constants.DATA).getDouble(field_name) +
                window.get(1).getJSONObject(Constants.DATA).getDouble(field_name) +
                window.get(2).getJSONObject(Constants.DATA).getDouble(field_name) +
                window.get(3).getJSONObject(Constants.DATA).getDouble(field_name) +
                window.get(4).getJSONObject(Constants.DATA).getDouble(field_name)) / Constants.JIANFENG_WINDOW_SIZE;
    }

    /*
     * 计算标准差
     */
    private static void calculateAndMark(List<JSONObject> window, String field_name, double avg, int index) {
        JSONObject qc_info = window.get(index).getJSONObject(Constants.QC_INFO);
        if (qc_info.containsKey(field_name)) {
            return;
        }

        double data = window.get(index).getJSONObject(Constants.DATA).getDouble(field_name);
        double threshold = Constants.SPIKE_THRESHOLD_FACTOR * data;
        boolean isAnomaly = data - avg >= threshold;

        if (isAnomaly) {
            qc_info.put(field_name, Constants.SPIKE_ANOMALY_MARK);
        }
    }
} 
package APP;

import com.alibaba.fastjson.JSONObject;

public class DataTransformer {
    private static final double TURBIDITY_FACTOR = 0.2419;
    private static final double CDOM_FACTOR = 0.0916;
    private static final double CHL_FACTOR = 0.0121;
    private static final int TURBIDITY_BASE = 50;
    private static final int CDOM_BASE = 50;
    private static final int CHL_BASE = 48;

    public static JSONObject transformData(JSONObject jsonObject) {
        JSONObject data = jsonObject.getJSONObject("data");
        
        data.put("turbidity", TURBIDITY_FACTOR * (data.getDouble("turbidity_count") - TURBIDITY_BASE));
        data.put("cdom", CDOM_FACTOR * (data.getDouble("cdom_count") - CDOM_BASE));
        data.put("chl", CHL_FACTOR * (data.getDouble("chl_count") - CHL_BASE));
        
        return jsonObject;
    }
} 
package APP;

import com.alibaba.fastjson.JSONObject;

public class DataTransformer {
    public static JSONObject transformData(JSONObject jsonObject) {
        JSONObject data = jsonObject.getJSONObject(Constants.DATA);
        
        data.put(Constants.TURBIDITY_FIELD, Constants.TURBIDITY_FACTOR * (data.getDouble("turbidity_count") - Constants.TURBIDITY_BASE));
        data.put(Constants.CDOM_FIELD, Constants.CDOM_FACTOR * (data.getDouble("cdom_count") - Constants.CDOM_BASE));
        data.put(Constants.CHL_FIELD, Constants.CHL_FACTOR * (data.getDouble("chl_count") - Constants.CHL_BASE));
        
        return jsonObject;
    }
} 
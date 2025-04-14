package APP;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;

public class StagnationTest {
    private static final String CHL_FIELD = "chl";
    private static final String CDOM_FIELD = "cdom";
    private static final String TURBIDITY_FIELD = "turbidity";

    public static void performStagnationTest(List<JSONObject> bufferlist, List<JSONObject> jfWindow, String field_name, boolean isFirstWindow) {
        if (jfWindow.size() < 5) {
            return;
        }

        if (isFirstWindow) {
            handleFirstWindow(bufferlist, jfWindow, field_name);
        }

        JSONObject currentData = jfWindow.get(2);
        handleCurrentData(bufferlist, currentData, field_name);
    }

    private static void handleFirstWindow(List<JSONObject> bufferlist, List<JSONObject> jfWindow, String field_name) {
        JSONObject firstData = jfWindow.get(0);
        bufferlist.add(firstData);

        JSONObject secondData = jfWindow.get(1);
        Double firstValue = firstData.getJSONObject("data").getDouble(field_name);
        Double secondValue = secondData.getJSONObject("data").getDouble(field_name);

        if (!secondValue.equals(firstValue)) {
            JSONObject first_qc = firstData.getJSONObject("qc_info");
            if (!first_qc.containsKey(field_name)) {
                first_qc.put(field_name, "1,11110");
            }
            bufferlist.clear();
            bufferlist.add(secondData);
        } else {
            bufferlist.add(secondData);
        }
    }

    private static void handleCurrentData(List<JSONObject> bufferlist, JSONObject currentData, String field_name) {
        if (bufferlist.isEmpty()) {
            bufferlist.add(currentData);
        } else {
            Double currentValue = currentData.getJSONObject("data").getDouble(field_name);
            Double bufferValue = bufferlist.get(0).getJSONObject("data").getDouble(field_name);

            if (currentValue.equals(bufferValue)) {
                bufferlist.add(currentData);
            } else {
                if (bufferlist.size() < 20) {
                    markSuccess(bufferlist, field_name);
                }
                bufferlist.clear();
                bufferlist.add(currentData);
            }
        }

        checkStagnationThreshold(bufferlist, field_name);
    }

    private static void markSuccess(List<JSONObject> bufferlist, String field_name) {
        for (JSONObject jsonObject : bufferlist) {
            JSONObject kz_qc = jsonObject.getJSONObject("qc_info");
            if (!kz_qc.containsKey(field_name)) {
                kz_qc.put(field_name, "1,10000");
            }
        }
    }

    private static void checkStagnationThreshold(List<JSONObject> bufferlist, String field_name) {
        if (bufferlist.size() == 20) {
            markStagnation(bufferlist, field_name);
        } else if (bufferlist.size() > 20) {
            JSONObject lastObject = bufferlist.get(bufferlist.size() - 1);
            JSONObject last_qc = lastObject.getJSONObject("qc_info");
            if (!last_qc.containsKey(field_name)) {
                last_qc.put(field_name, "4,11110");
            }
        }
    }

    private static void markStagnation(List<JSONObject> bufferlist, String field_name) {
        for (JSONObject jsonObject : bufferlist) {
            JSONObject current_qc = jsonObject.getJSONObject("qc_info");
            if (!current_qc.containsKey(field_name)) {
                current_qc.put(field_name, "4,11110");
            }
        }
    }
} 
package APP;

import com.alibaba.fastjson.JSONObject;
import java.util.List;

/*
 * 卡滞测试
 */
public class StagnationTest {

    public static void performStagnationTest(List<JSONObject> bufferList, List<JSONObject> jfWindow, String field_name, boolean isFirstWindow) {
        if (jfWindow.size() < Constants.JIANFENG_WINDOW_SIZE) {
            return;
        }

        if (isFirstWindow) {
            handleFirstWindow(bufferList, jfWindow, field_name);
        }

        JSONObject currentData = jfWindow.get(2);
        handleCurrentData(bufferList, currentData, field_name);
    }

    private static void handleFirstWindow(List<JSONObject> bufferList, List<JSONObject> jfWindow, String field_name) {
        JSONObject firstData = jfWindow.get(0);
        bufferList.add(firstData);

        JSONObject secondData = jfWindow.get(1);
        Double firstValue = firstData.getJSONObject(Constants.DATA).getDouble(field_name);
        Double secondValue = secondData.getJSONObject(Constants.DATA).getDouble(field_name);

        if (!secondValue.equals(firstValue)) {
            JSONObject first_qc = firstData.getJSONObject("qc_info");
            if (!first_qc.containsKey(field_name)) {
                first_qc.put(field_name, Constants.FIRST_WINDOW_SUCCESS_MARK);
            }
            bufferList.clear();
            bufferList.add(secondData);
        } else {
            bufferList.add(secondData);
        }
    }

    private static void handleCurrentData(List<JSONObject> bufferList, JSONObject currentData, String field_name) {
        if (bufferList.isEmpty()) {
            bufferList.add(currentData);
        } else {
            Double currentValue = currentData.getJSONObject(Constants.DATA).getDouble(field_name);
            Double bufferValue = bufferList.get(0).getJSONObject(Constants.DATA).getDouble(field_name);

            if (currentValue.equals(bufferValue)) {
                bufferList.add(currentData);
            } else {
                if (bufferList.size() < Constants.STAGNATION_WINDOW_SIZE) {
                    markSuccess(bufferList, field_name);
                }
                bufferList.clear();
                bufferList.add(currentData);
            }
        }

        checkStagnationThreshold(bufferList, field_name);
    }

    private static void markSuccess(List<JSONObject> bufferList, String field_name) {
        for (JSONObject jsonObject : bufferList) {
            JSONObject kz_qc = jsonObject.getJSONObject("qc_info");
            if (!kz_qc.containsKey(field_name)) {
                kz_qc.put(field_name, Constants.STAGNATION_SUCCESS_MARK);
            }
        }
    }

    private static void checkStagnationThreshold(List<JSONObject> bufferList, String field_name) {
        if (bufferList.size() == Constants.STAGNATION_WINDOW_SIZE) {
            markStagnation(bufferList, field_name);
        } else if (bufferList.size() > Constants.STAGNATION_WINDOW_SIZE) {
            JSONObject lastObject = bufferList.get(bufferList.size() - 1);
            JSONObject last_qc = lastObject.getJSONObject("qc_info");
            if (!last_qc.containsKey(field_name)) {
                last_qc.put(field_name, Constants.STAGNATION_FAILURE_MARK);
            }
        }
    }

    private static void markStagnation(List<JSONObject> bufferList, String field_name) {
        for (JSONObject jsonObject : bufferList) {
            JSONObject current_qc = jsonObject.getJSONObject("qc_info");
            if (!current_qc.containsKey(field_name)) {
                current_qc.put(field_name, Constants.STAGNATION_FAILURE_MARK);
            }
        }
    }
} 
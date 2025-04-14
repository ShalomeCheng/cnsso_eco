package APP.MultipleClass;

import com.alibaba.fastjson.JSONObject;
import java.util.List;

import org.apache.flink.api.common.state.ValueState;
/*
 * 卡滞测试
 */
public class StagnationTest {

    public static void performStagnationTest(List<JSONObject> allMessages, String field_name,
            ValueState<Double> lastValueState, ValueState<Integer> duplicationCountState,
            Boolean isFirstWindow) throws Exception 
        {
        // 完成第一次尖峰测试后，才开启卡滞测试的流程
        if (allMessages.size() < Constants.JIANFENG_WINDOW_SIZE) {
            return;
        }

        Integer duplicateDataCount = duplicationCountState.value();
        Double lastValue = lastValueState.value();

        // 第一次进入卡滞测试，需要对第1,2条数据特殊处理。
        if (isFirstWindow) {
            // 第一条数据, 初始化 value和count
            lastValue = allMessages.get(0).getJSONObject("data").getDouble(field_name);
            duplicateDataCount = 1;

            // 第二条数据，需要和上一条数据的相应数值进行比较
            Double secondValue = allMessages.get(1).getJSONObject("data").getDouble(field_name);
            // 第一二条数据值不相同，说明第一条数据一定不卡滞
            if (!secondValue.equals(lastValue)) {
                markSuccess(allMessages.subList(0, 1), field_name);
                // 重置lastValue
                lastValue = secondValue;
                duplicateDataCount = 1;
            } else {
                // 累计
                duplicateDataCount++;
            }
        }

        // 默认只对尖峰测试窗口中的第n-2条数据做卡滞测试
        int targetIndex = allMessages.size() - 3;
        JSONObject currentData = allMessages.get(targetIndex);

        // 比较当前数据与buffer数组元素值是否相同
        Double currentValue = currentData.getJSONObject("data").getDouble(field_name);
        if (currentValue.equals(lastValue)) {
            // 若值相同，则累计
            duplicateDataCount++;
        } else {
            // 若值不相同，相同元素计数小于20，则说明前面的数据一定不卡滞，需要标记【成功】
            if (duplicateDataCount < 20) {
                // 下标为（targetIndex - duplicateDataCount) - (targetIndex - 1)的数据标记成功
                markSuccess(allMessages.subList(Math.max(0, targetIndex - duplicateDataCount), targetIndex),
                        field_name);
            }

            // 重置buffer元素，添加当前数据重新开始累计
            lastValue = currentValue;
            duplicateDataCount = 1;
        }

        // 检查buffer内数据是否累计到卡滞阈值，决定是否要标记【卡滞】
        if (duplicateDataCount == 20) {
            // 首次累计到阈值，标为 0 - targetIndex条数据标记卡滞
            markStagnation(allMessages.subList(0, targetIndex + 1), field_name);

        } else if (duplicateDataCount > 20) {
            // 第20位以后的数据，标记当前数据即可
            // 下标为 targetIndex 的数据标记卡滞
            markStagnation(allMessages.subList(targetIndex, targetIndex + 1), field_name);
        }

        // 回写数据到State
        lastValueState.update(lastValue);
        duplicationCountState.update(duplicateDataCount);
    }

    private static void markSuccess(List<JSONObject> bufferList, String field_name) {
        for (JSONObject jsonObject : bufferList) {
            JSONObject kz_qc = jsonObject.getJSONObject("qc_info");
            //没通过仪器和尖峰测试的数据不再标记；以及标记过的数据不再标记，避免错误
            if (!kz_qc.containsKey(field_name)) {
                System.out.println("标记成功");
                kz_qc.put(field_name, Constants.STAGNATION_SUCCESS_MARK);
            }
        }
    }

    private static void markStagnation(List<JSONObject> bufferList, String field_name) {
        for (JSONObject jsonObject : bufferList) {
            JSONObject current_qc = jsonObject.getJSONObject(Constants.QC_INFO);
            if (!current_qc.containsKey(field_name)) {
                System.out.println("标记卡滞");
                current_qc.put(field_name, Constants.STAGNATION_FAILURE_MARK);
            }
        }
    }
} 
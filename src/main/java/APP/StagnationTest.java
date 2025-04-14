package APP;

import com.alibaba.fastjson.JSONObject;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * 卡滞测试
 */
public class StagnationTest {

    private static final Logger logger = LoggerFactory.getLogger(StagnationTest.class);
    public static void performStagnationTest(List<JSONObject> allMessages, String field_name, boolean windowStagnationFlag) 
    {
        //窗口达到22时才触发卡滞计算
        if(allMessages.size() < Constants.TOTAL_WINDOW_SIZE)
            return;

        //最多计算到n-2位，即第20位的数据
        int targetIndex = allMessages.size() - 3; // n-2的位置

        //从当前位置往前遍历，统计连续相同数据的个数
        int sameCount = 1;
        JSONObject currentData = allMessages.get(targetIndex);
        Double currentValue = currentData.getJSONObject(Constants.DATA).getDouble(field_name);
        for(int i = targetIndex - 1; i >= 0; i--)
        {
            Double preValue = allMessages.get(i).getJSONObject(Constants.DATA).getDouble(field_name);
            if(preValue.equals(currentValue))
            {
                sameCount++;
                //如果前一个窗口是卡滞状态，那么第20位以后相同的数据，直接可以判断为卡滞
                if(windowStagnationFlag == true)
                {
                    break;
                }
            }else
            {
                windowStagnationFlag = false;
                break;
            }
        }

        if(windowStagnationFlag)
        {
            System.out.println("标记n-2条卡滞");
            //targetIndex位直接标记卡滞
            markStagnation(allMessages.subList(targetIndex, targetIndex + 1), field_name);

        }
        else if(sameCount == Constants.STAGNATION_WINDOW_SIZE)
        {
            //20位全部标记卡滞
            System.out.println("当前窗口20条数据卡滞");
            markStagnation(allMessages.subList(0, targetIndex), field_name);
            windowStagnationFlag = true;
        }
        else
        {
            //0 - (targetIndex - sameCount)位标记成功
            System.out.println("标记部分卡滞");
            markSuccess(allMessages.subList(0, targetIndex - sameCount), field_name);
        }
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
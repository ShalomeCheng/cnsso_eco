package APP.OneClass;

import Utils.KafkaUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import APP.Constants;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class OneClass {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String CHL_FIELD = "chl";
    private static final String CDOM_FIELD = "cdom";
    private static final String TURBIDITY_FIELD = "turbidity";
    private static final int JF_WINDOW_SIZE = 5;
    private static final int TOTAL_WINDOW_SIZE = 22;

    private static final Logger logger = LoggerFactory.getLogger(OneClass.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        // 获取环境
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtils.getKafkaSource("iot_send_eco", "test");
        // 获取消费者
        // DataStreamSource<String> jsonStrDs =
        // env.addSource(kafkaSource).setParallelism(1);
        DataStreamSource<String> localhost = env.socketTextStream("hadoop102", 7777);
        // 接入流
        SingleOutputStreamOperator<String> filteredDs = localhost // 过滤
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (value == null || value.trim().isEmpty()) {
                            return false; // 过滤空字符串
                        }
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            if (jsonObj == null || !jsonObj.containsKey("data")) {
                                return false; // 过滤没有 "data" 字段的数据
                            }

                            JSONObject data = jsonObj.getJSONObject("data");
                            if (data == null) {
                                return false;
                            }

                            // 检查 turbidity_count、chl_count、cdom_count 是否非空
                            return data.get("turbidity_count") != null
                                    && data.get("chl_count") != null
                                    && data.get("cdom_count") != null;
                        } catch (Exception e) {
                            return false;
                        }
                    }
                }).setParallelism(1);

        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = filteredDs
                .map(new RichMapFunction<String, JSONObject>() {

                    private static final double TURBIDITY_FACTOR = 0.2419;
                    private static final double CDOM_FACTOR = 0.0916;
                    private static final double CHL_FACTOR = 0.0121;
                    private static final int TURBIDITY_BASE = 50;
                    private static final int CDOM_BASE = 50;
                    private static final int CHL_BASE = 48;

                    @Override
                    public JSONObject map(String value) throws Exception {

                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject data = jsonObject.getJSONObject("data");

                        data.put("turbidity", TURBIDITY_FACTOR * (data.getDouble("turbidity_count") - TURBIDITY_BASE));
                        data.put("cdom", CDOM_FACTOR * (data.getDouble("cdom_count") - CDOM_BASE));
                        data.put("chl", CHL_FACTOR * (data.getDouble("chl_count") - CHL_BASE));

                        return jsonObject;

                    }
                }).setParallelism(1)
                .keyBy(a -> a.getJSONObject("profile").getString("device_id"));
        // jsonObjectStringKeyedStream.print();
        // SingleOutputStreamOperator<String> mapStrDs =
        // jsonObjectStringKeyedStream.map(a -> a.toString());

        // mapStrDs.print();

        // FlinkKafkaProducer<String> stringFlinkKafkaProducer =
        // KafkaUtils.sendKafkaDs("dc_std_eco", "test");

        // mapStrDs.addSink(stringFlinkKafkaProducer);
        SingleOutputStreamOperator<JSONObject> process = jsonObjectStringKeyedStream
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ListState<JSONObject> count_All;
                    ValueState<Boolean> CHL_WindowStagnationFlag;
                    ValueState<Boolean> CDOM_WindowStagnationFlag;
                    ValueState<Boolean> TURBIDITY_WindowStagnationFlag;

                    final Double chl_SmallSize = 50.0;
                    final Double chl_BigSize = 4130.0;
                    final Double fdom_SmallSize = 50.0;
                    final Double fdom_BigSize = 4130.0;
                    final Double ntu_SmallSiz = 50.0;
                    final Double ntu_BigSize = 4130.0;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        count_All = getRuntimeContext()
                                .getListState(new ListStateDescriptor<JSONObject>("count_All", JSONObject.class));
                        CHL_WindowStagnationFlag = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("booleanState", Types.BOOLEAN));
                        CDOM_WindowStagnationFlag = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("booleanState", Types.BOOLEAN));
                        TURBIDITY_WindowStagnationFlag = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("booleanState", Types.BOOLEAN));
                    }

                    @Override
                    public void processElement(JSONObject value,
                            KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out)
                            throws Exception {

                        // 初始化数据
                        JSONObject jsdata = value.getJSONObject("data");
                        JSONObject qc_info = new JSONObject(); // 新建一个tag js对象 用来存放qc_info
                        value.put("qc_info", qc_info);

                        Double chl_count = jsdata.getDouble("chl_count");
                        Double turbidity_count = jsdata.getDouble("turbidity_count");
                        Double cdom_count = jsdata.getDouble("cdom_count");
                        // 仪器测试不通过,直接打标签 并且当前数据不需要再进行对应的尖峰测试，卡滞测试
                        if (chl_count > chl_BigSize || chl_count < chl_SmallSize) {
                            qc_info.put(CHL_FIELD, "4,110");
                        }
                        if (turbidity_count > ntu_BigSize || turbidity_count < ntu_SmallSiz) {
                            qc_info.put(TURBIDITY_FIELD, "4,110");

                        }
                        if (cdom_count > fdom_BigSize || cdom_count < fdom_SmallSize) {
                            qc_info.put(CDOM_FIELD, "4,110");
                        }

                        // 添加到缓存
                        count_All.add(value);
                        setCountSize(count_All, TOTAL_WINDOW_SIZE);

                        List<JSONObject> allMessages = new ArrayList<>();
                        count_All.get().forEach(allMessages::add);

                        // 当有足够消息时(n >= 5)，才处理n-2的消息
                        if (allMessages.size() < JF_WINDOW_SIZE) {
                            return;
                        }

                        // 4.1 处理尖峰测试
                        List<JSONObject> jf_window = allMessages.subList(
                                allMessages.size() - JF_WINDOW_SIZE,
                                allMessages.size());

                        boolean firstWindow = allMessages.size() == JF_WINDOW_SIZE;
                        JianFeng_Test(jf_window, CHL_FIELD, firstWindow);
                        JianFeng_Test(jf_window, CDOM_FIELD, firstWindow);
                        JianFeng_Test(jf_window, TURBIDITY_FIELD, firstWindow);

                        Kazhi_test(allMessages, CHL_FIELD, CHL_WindowStagnationFlag);
                        Kazhi_test(allMessages, CDOM_FIELD, CDOM_WindowStagnationFlag);
                        Kazhi_test(allMessages, TURBIDITY_FIELD, TURBIDITY_WindowStagnationFlag);

                        // 4.3 更新状态
                        count_All.update(allMessages);

                        if (allMessages.size() == TOTAL_WINDOW_SIZE) {
                            JSONObject firstData = allMessages.get(0);
                            System.out.println(firstData.toString());

                            out.collect(firstData);
                        }

                    }

                    private void setCountSize(ListState<JSONObject> stateCountBuffer, int size) throws Exception {
                        List<JSONObject> window = new ArrayList<>();
                        stateCountBuffer.get().forEach(window::add);
                        if (window.size() > size) {
                            // 滑动窗口，每次向后移一位
                            window = new ArrayList<>(window.subList(window.size() - size, window.size()));
                            stateCountBuffer.update(window);
                        }
                    }

                    // region 尖峰测试
                    private void JianFeng_Test(List<JSONObject> jf_CountList, String field_name, boolean firstWindow)
                            throws Exception {
                        // 数据量小于5
                        if (jf_CountList.size() < 5) {
                            return;
                        }

                        // 计算窗口内平均值
                        double avg = CalAvg(jf_CountList, field_name);

                        // 针对第1,2条数据，如果通过了仪器测试，则需要进行尖峰测试
                        if (firstWindow) {
                            CalAndMark(jf_CountList, field_name, avg, 0);
                            CalAndMark(jf_CountList, field_name, avg, 1);
                        }

                        CalAndMark(jf_CountList, field_name, avg, 2);
                    }

                    private double CalAvg(List<JSONObject> window, String filed_name) {
                        // 计算窗口内平均值（RI）
                        // 存储五条数据 里面有三个值 chl cdom 和ntu 分别计算 这三个值的尖峰 n-2 n-1 n n+1 n+2 取出来算一下
                        double avg = (window.get(0).getJSONObject("data").getDouble(filed_name) +
                                window.get(1).getJSONObject("data").getDouble(filed_name) +
                                window.get(2).getJSONObject("data").getDouble(filed_name) +
                                window.get(3).getJSONObject("data").getDouble(filed_name) +
                                window.get(4).getJSONObject("data").getDouble(filed_name) / 5.0);
                        return avg;
                    }

                    private void CalAndMark(List<JSONObject> window, String field_name, double avg, int index) {
                        JSONObject qc_info = window.get(index).getJSONObject("qc_info");
                        if (qc_info.containsKey(field_name)) {
                            // 如果仪器测试没通过，不进行尖峰测试
                            return;

                        }

                        // 获取index对应data
                        double data = window.get(index).getJSONObject("data").getDouble(field_name);//
                        // 异常检测：|R - RI| > N*R
                        double threshold = 0.3 * data;
                        boolean isAnomaly = data - avg >= threshold;
                        // boolean isAnomaly = data<3;

                        if (isAnomaly) {
                            // System.out.println("尖峰测试异常 " + field_name + "值出现问题");
                            qc_info.put(field_name, "3,1110");
                        }
                    }
                    // endregion

                    // region 卡滞测试
                    /*
                     * 卡滞测试
                     */
                    private void Kazhi_test(List<JSONObject> allMessages, String field_name,
                            ValueState<Boolean> windowStagnationFlag) throws Exception {
                        // 窗口达到22时才触发卡滞计算
                        if (allMessages.size() < TOTAL_WINDOW_SIZE)
                            return;

                        // 最多计算到n-2位，即第20位的数据
                        int targetIndex = allMessages.size() - 3; // n-2的位置

                        // 从当前位置往前遍历，统计连续相同数据的个数
                        int sameCount = 1;
                        JSONObject currentData = allMessages.get(targetIndex);
                        Double currentValue = currentData.getJSONObject("data").getDouble(field_name);
                        for (int i = targetIndex - 1; i >= 0; i--) {
                            Double preValue = allMessages.get(i).getJSONObject("data").getDouble(field_name);
                            if (preValue.equals(currentValue)) {
                                sameCount++;
                                // 如果前一个窗口是卡滞状态，那么第20位以后相同的数据，直接可以判断为卡滞
                                if (Boolean.TRUE.equals(windowStagnationFlag.value())) {
                                    break;
                                }
                            } else {
                                windowStagnationFlag.update(false);
                                break;
                            }
                        }

                        if (Boolean.TRUE.equals(windowStagnationFlag.value()))
                        {
                            System.out.println("标记n-2条卡滞");
                            // targetIndex位直接标记卡滞
                            markStagnation(allMessages.subList(targetIndex, targetIndex + 1), field_name);

                        } else if (sameCount == 20) {
                            // 20位全部标记卡滞
                            System.out.println("当前窗口20条数据卡滞");
                            markStagnation(allMessages.subList(0, targetIndex), field_name);
                            windowStagnationFlag.update(true);
                        } else {
                            // 0 - (targetIndex - sameCount)位标记成功
                            System.out.println("标记部分卡滞");
                            markSuccess(allMessages.subList(0, targetIndex - sameCount), field_name);
                        }
                    }

                    private void markSuccess(List<JSONObject> bufferList, String field_name) {
                        for (JSONObject jsonObject : bufferList) {
                            JSONObject kz_qc = jsonObject.getJSONObject("qc_info");
                            // 没通过仪器和尖峰测试的数据不再标记；以及标记过的数据不再标记，避免错误
                            if (!kz_qc.containsKey(field_name)) {
                                System.out.println("标记成功");
                                kz_qc.put(field_name, "1,10000");
                            }
                        }
                    }

                    private void markStagnation(List<JSONObject> bufferList, String field_name) {
                        for (JSONObject jsonObject : bufferList) {
                            JSONObject current_qc = jsonObject.getJSONObject("qc_info");
                            if (!current_qc.containsKey(field_name)) {
                                System.out.println("标记卡滞");
                                current_qc.put(field_name, "4,11110");
                            }
                        }
                    }

                    // endregion

                });
        FlinkKafkaProducer<String> stringFlinkKafkaProducer1 = KafkaUtils.sendKafkaDs("dc_qc_eco", "test");
        // process.print().setParallelism(1);
        // process.map(a -> a.toString()).addSink(stringFlinkKafkaProducer1);
        SingleOutputStreamOperator<String> map = process.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObject) throws Exception {
                String data = jsonObject.toString();

                return data;
            }
        });
        // map.addSink(stringFlinkKafkaProducer1);
        // process.print();
        // map.print();
        // process.print();
        // process.map(a->a.toString()).print();
        env.execute();

    }

}

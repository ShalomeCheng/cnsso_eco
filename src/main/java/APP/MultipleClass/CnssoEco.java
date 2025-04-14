package APP.MultipleClass;

import Utils.KafkaUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class CnssoEco {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final Logger logger = LoggerFactory.getLogger(CnssoEco.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());

        DataStreamSource<String> localhost = env.socketTextStream(Constants.SOCKET_HOST, Constants.SOCKET_PORT);

        // 1. 数据过滤与分类
        SingleOutputStreamOperator<String> filteredDs = localhost
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (value == null || value.trim().isEmpty()) {
                            return false;
                        }
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            if (jsonObj == null || !jsonObj.containsKey(Constants.DATA)) {
                                return false;
                            }

                            JSONObject data = jsonObj.getJSONObject(Constants.DATA);
                            if (data == null) {
                                return false;
                            }

                            return data.get("turbidity_count") != null
                                    && data.get("chl_count") != null
                                    && data.get("cdom_count") != null;
                        } catch (Exception e) {
                            return false;
                        }
                    }
                }).setParallelism(1);

        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = filteredDs
                .map(value -> DataTransformer.transformData(JSON.parseObject(value)))
                .setParallelism(1)
                .keyBy(a -> a.getJSONObject("profile").getString("device_id"));

        // 2. 数据处理
        SingleOutputStreamOperator<JSONObject> process = jsonObjectStringKeyedStream
                .process(new QualityControlProcessFunction());

        // 3. 发送消息到下游
        FlinkKafkaProducer<String> stringFlinkKafkaProducer = KafkaUtils.sendKafkaDs(Constants.KAFKA_TOPIC_QC,
                Constants.KAFKA_GROUP_ID);

        process.map(JSONObject::toString)
                .addSink(stringFlinkKafkaProducer);

        env.execute();
    }

    private static class QualityControlProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        ListState<JSONObject> count_All;
        // 记录targetIndex-1条所在的窗口是否形成卡滞
        ValueState<Boolean> CHL_WindowStagnationFlag;
        ValueState<Boolean> CDOM_WindowStagnationFlag;
        ValueState<Boolean> TURBIDITY_WindowStagnationFlag;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            count_All = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("count_All", JSONObject.class));
            CHL_WindowStagnationFlag = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("booleanState", Types.BOOLEAN));
            CDOM_WindowStagnationFlag = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("booleanState", Types.BOOLEAN));
            TURBIDITY_WindowStagnationFlag = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("booleanState", Types.BOOLEAN));
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

            // 仪器测试
            QualityControl.performInstrumentTest(value);

            // 添加到缓存
            count_All.add(value);
            setCountSize(count_All, Constants.TOTAL_WINDOW_SIZE);

            List<JSONObject> allMessages = new ArrayList<>();
            count_All.get().forEach(allMessages::add);

            // 当有足够消息时(n >= 5)，才处理n-2的消息
            if (allMessages.size() < Constants.JIANFENG_WINDOW_SIZE) {
                return;
            }

            // 4.1 处理尖峰测试
            List<JSONObject> jf_window = allMessages.subList(
                    allMessages.size() - Constants.JIANFENG_WINDOW_SIZE,
                    allMessages.size());

            boolean firstWindow = allMessages.size() == Constants.JIANFENG_WINDOW_SIZE;
            QualityControl.performJianFengTest(jf_window, Constants.CHL_FIELD, firstWindow);
            QualityControl.performJianFengTest(jf_window, Constants.CDOM_FIELD, firstWindow);
            QualityControl.performJianFengTest(jf_window, Constants.TURBIDITY_FIELD, firstWindow);

            StagnationTest.performStagnationTest(allMessages, Constants.CHL_FIELD, CHL_WindowStagnationFlag);
            StagnationTest.performStagnationTest(allMessages, Constants.CDOM_FIELD, CDOM_WindowStagnationFlag);
            StagnationTest.performStagnationTest(allMessages, Constants.TURBIDITY_FIELD, TURBIDITY_WindowStagnationFlag);

            // 4.3 更新状态
            count_All.update(allMessages);

            if (allMessages.size() == Constants.TOTAL_WINDOW_SIZE) {
                JSONObject firstData = allMessages.get(0);
                System.out.println(firstData.toString());

                out.collect(firstData);
            }
        }

        private static void setCountSize(ListState<JSONObject> stateCountBuffer, int size) throws Exception {
            List<JSONObject> window = new ArrayList<>();
            stateCountBuffer.get().forEach(window::add);
            if (window.size() > size) {
                window = new ArrayList<>(window.subList(window.size() - size, window.size()));
                stateCountBuffer.update(window);
            }
        }

    }
}

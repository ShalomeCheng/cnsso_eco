package APP;

import Utils.KafkaUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
    private static final String CHL_FIELD = "chl";
    private static final String CDOM_FIELD = "cdom";
    private static final String TURBIDITY_FIELD = "turbidity";
    private static final Logger logger = LoggerFactory.getLogger(CnssoEco.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());

        DataStreamSource<String> localhost = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<String> filteredDs = localhost
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (value == null || value.trim().isEmpty()) {
                            return false;
                        }
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            if (jsonObj == null || !jsonObj.containsKey("data")) {
                                return false;
                            }

                            JSONObject data = jsonObj.getJSONObject("data");
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

        SingleOutputStreamOperator<JSONObject> process = jsonObjectStringKeyedStream
                .process(new QualityControlProcessFunction());

        FlinkKafkaProducer<String> stringFlinkKafkaProducer = KafkaUtils.sendKafkaDs("dc_qc_eco", "test");
        
        process.map(JSONObject::toString)
               .addSink(stringFlinkKafkaProducer);

        env.execute();
    }

    private static class QualityControlProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private ListState<JSONObject> count_All;
        private ListState<JSONObject> jf_CountListState;
        private ListState<JSONObject> cdomCountBuffer;
        private ListState<JSONObject> chlCountBuffer;
        private ListState<JSONObject> turbidityCountBuffer;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            jf_CountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("jf_CountListState", JSONObject.class));
            count_All = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("count_All", JSONObject.class));
            cdomCountBuffer = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("cdomCountBuffer", JSONObject.class));
            chlCountBuffer = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("chlCountBuffer", JSONObject.class));
            turbidityCountBuffer = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("turbidityCountBuffer", JSONObject.class));
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
            QualityControl.performInstrumentTest(value);

            count_All.add(value);
            setCountSize(count_All, 22);
            List<JSONObject> count_All_List = new ArrayList<>();
            count_All.get().forEach(count_All_List::add);
            boolean firstWindow = count_All_List.size() == 5;

            jf_CountListState.add(value);
            setCountSize(jf_CountListState, 5);
            List<JSONObject> jf_CountList = new ArrayList<>();
            jf_CountListState.get().forEach(jf_CountList::add);

            QualityControl.performJianFengTest(jf_CountList, CHL_FIELD, firstWindow);
            QualityControl.performJianFengTest(jf_CountList, CDOM_FIELD, firstWindow);
            QualityControl.performJianFengTest(jf_CountList, TURBIDITY_FIELD, firstWindow);

            List<JSONObject> chlBuffer = new ArrayList<>();
            chlCountBuffer.get().forEach(chlBuffer::add);
            StagnationTest.performStagnationTest(chlBuffer, jf_CountList, CHL_FIELD, firstWindow);
            chlCountBuffer.update(chlBuffer);

            List<JSONObject> cdomBuffer = new ArrayList<>();
            cdomCountBuffer.get().forEach(cdomBuffer::add);
            StagnationTest.performStagnationTest(cdomBuffer, jf_CountList, CDOM_FIELD, firstWindow);
            cdomCountBuffer.update(cdomBuffer);

            List<JSONObject> turbidityBuffer = new ArrayList<>();
            turbidityCountBuffer.get().forEach(turbidityBuffer::add);
            StagnationTest.performStagnationTest(turbidityBuffer, jf_CountList, TURBIDITY_FIELD, firstWindow);
            turbidityCountBuffer.update(turbidityBuffer);

            jf_CountListState.update(jf_CountList);
            count_All.update(count_All_List);

            if (count_All_List.size() == 22) {
                JSONObject firstData = jf_CountList.get(0);
                System.out.println(firstData.toString());
                out.collect(firstData);
            }
        }

        private void setCountSize(ListState<JSONObject> stateCountBuffer, int size) throws Exception {
            List<JSONObject> window = new ArrayList<>();
            stateCountBuffer.get().forEach(window::add);
            if (window.size() > size) {
                window = new ArrayList<>(window.subList(window.size() - size, window.size()));
                stateCountBuffer.update(window);
            }
        }
    }
}









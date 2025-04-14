package APP.test;

import Utils.KafkaUtils;
import com.alibaba.fastjson.JSONObject;

import APP.CnssoEco;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class SerTest {
    private static final Logger logger = LoggerFactory.getLogger(CnssoEco.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtils.getKafkaSource("iot_send_eco", "test");
        //获取消费者
        DataStreamSource<String> jsonStrDs = env.addSource(kafkaSource).setParallelism(1);
//        DataStreamSource<String> localhost = env.socketTextStream("hadoop102", 7777);
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonStrDs.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                return jsonObject;
            }
        }).keyBy(a -> a.getJSONObject("profile").getString("device_id"));

        SingleOutputStreamOperator<JSONObject> process = jsonObjectStringKeyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ListState<JSONObject> jf_CountListState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                jf_CountListState = getRuntimeContext().getListState(new ListStateDescriptor<JSONObject>("jf_CountListState", JSONObject.class));

            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                // JSONObject jsdata = value.getJSONObject("data");
                JSONObject qc_info = new JSONObject(); //新建一个tag js对象 用来存放qc_info
                value.put("qc_info", qc_info);
                qc_info.put("cxlcxlcxlcxlcxlcxl", 11111);
                logger.info(value.toString());
                jf_CountListState.add(value);


                List<JSONObject> bufferlist = new ArrayList<>();
                jf_CountListState.get().forEach(bufferlist::add);
                for (JSONObject jsonObject : bufferlist) {

                    logger.info(jsonObject.toString());
                    JSONObject qc_info1 = jsonObject.getJSONObject("qc_info");
                    qc_info1.put("577", "sssssssssssssssssssssssssssssssssssssssssssss");
                }
                List<JSONObject> bufferlist1 = new ArrayList<>();
                jf_CountListState.get().forEach(bufferlist1::add);
                for (JSONObject jsonObject : bufferlist1) {
                    logger.info(jsonObject.toString()+"kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk");
                }


            }
        });
        env.execute();

    }
}

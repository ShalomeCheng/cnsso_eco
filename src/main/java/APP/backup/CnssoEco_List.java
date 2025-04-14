package APP.backup;

import Utils.KafkaUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
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

public class CnssoEco_List {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String CHL_FIELD = "chl";
    private static final String CDOM_FIELD = "cdom";
    private static final String TURBIDITY_FIELD = "turbidity";
    private static final Logger logger = LoggerFactory.getLogger(CnssoEco_List.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取环境
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtils.getKafkaSource("iot_send_eco", "test");
        //获取消费者
        DataStreamSource<String> jsonStrDs = env.addSource(kafkaSource).setParallelism(1);
//        DataStreamSource<String> localhost = env.socketTextStream("hadoop102", 7777);
        //接入流
        SingleOutputStreamOperator<String> filteredDs = jsonStrDs  //过滤
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


        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = filteredDs.map(new RichMapFunction<String, JSONObject>() {

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
        //SingleOutputStreamOperator<String> mapStrDs = jsonObjectStringKeyedStream.map(a -> a.toString());

        //mapStrDs.print();

        //FlinkKafkaProducer<String> stringFlinkKafkaProducer = KafkaUtils.sendKafkaDs("dc_std_eco", "test");


        // mapStrDs.addSink(stringFlinkKafkaProducer);
        SingleOutputStreamOperator<JSONObject> process = jsonObjectStringKeyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
      List<JSONObject> count_All;

            List<JSONObject> jf_CountListState;  //尖峰测试数据状态
            ValueState<Long> lastTimeStrState; //用来计算时间连续性检测

            //用来存放进行卡滞测试的缓存数据
            List<JSONObject> cdomCountBuffer;  //卡滞测试数据状态
            List<JSONObject> chlCountBuffer;   //卡滞测试数据状态
            List<JSONObject> turbidityCountBuffer; //卡滞测试数据状态

            final Double chl_SmallSize = 50.0;
            final Double chl_BigSize = 4130.0;
            final Double fdom_SmallSize = 50.0;
            final Double fdom_BigSize = 4130.0;
            final Double ntu_SmallSiz = 50.0;
            final Double ntu_BigSize = 4130.0;


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lastTimeStrState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTimeStr", Types.LONG));
                //count的状态 用来存放仪器检测的数据
                jf_CountListState = new ArrayList<>();
                count_All=new ArrayList<>();
                //用来计算 存储卡滞的数据
                cdomCountBuffer =new ArrayList<>();
                chlCountBuffer = new ArrayList<>();
                turbidityCountBuffer = new ArrayList<>();
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                //初始化数据
                JSONObject jsdata = value.getJSONObject("data");
                JSONObject qc_info = new JSONObject(); //新建一个tag js对象 用来存放qc_info
                value.put("qc_info", qc_info);

                Double chl_count = jsdata.getDouble("chl_count");
                Double turbidity_count = jsdata.getDouble("turbidity_count");
                Double cdom_count = jsdata.getDouble("cdom_count");
                //仪器测试不通过,直接打标签 并且当前数据不需要再进行对应的尖峰测试，卡滞测试
                if (chl_count > chl_BigSize || chl_count < chl_SmallSize) {
                    qc_info.put(CHL_FIELD, "4,110");
                }
                if (turbidity_count > ntu_BigSize || turbidity_count < ntu_SmallSiz) {
                    qc_info.put(TURBIDITY_FIELD, "4,110");

                }
                if (cdom_count > fdom_BigSize || cdom_count < fdom_SmallSize) {
                    qc_info.put(CDOM_FIELD, "4,110");
                }

                //缓存进来的额数据
                count_All.add(value);
                //设置滑动窗口大小,20
                count_All = setCountSize(count_All, 22);
                //标记是否第一次达成尖峰测试的大小为5的窗口，后续需要对第1,2,3条数据特殊处理；每次尖峰测试窗口默认计算第3位的数据，窗口每次向后滑动1位
                boolean firstWindow = count_All.size() == 5;


                //尖峰测试使用的缓存窗口，三个属性共用,大小为5
                jf_CountListState.add(value);

                jf_CountListState = setCountSize(jf_CountListState, 5);


                //第n-2条数据计算尖峰测试
                JianFeng_Test(jf_CountListState, CHL_FIELD, firstWindow);
                JianFeng_Test(jf_CountListState, CDOM_FIELD, firstWindow);
                JianFeng_Test(jf_CountListState, TURBIDITY_FIELD, firstWindow);

                //第n-2条数据计算卡滞测试
                Kazhi_test(chlCountBuffer, jf_CountListState, CHL_FIELD, firstWindow);
                Kazhi_test(cdomCountBuffer, jf_CountListState, CDOM_FIELD, firstWindow);
                Kazhi_test(turbidityCountBuffer, jf_CountListState, TURBIDITY_FIELD, firstWindow);

                //形成大小20的固定窗口后，每次发送第一条消息
                //todo: 需要考虑数据不足20条，或者无后续数据时窗口内消息的发送方案
                //System.out.println("1111111111111111111111111");
                if (count_All.size() == 22) {

                    JSONObject firstData = count_All.get(0);
                    //System.out.println("发送第一条数据");
                    //System.out.println(firstData);
//                    System.out.println("标记111111111");
                       System.out.println(firstData.toString());
//                    logger.info(firstData.toString());
                    out.collect(firstData);
                }

            }

            private void JianFeng_Test(List<JSONObject> jf_CountList, String field_name, boolean firstWindow) throws Exception {
                //数据量小于5
                if (jf_CountList.size() < 5) {
                    return;
                }

                //计算窗口内平均值
                double avg = CalAvg(jf_CountList, field_name);

                //针对第1,2条数据，如果通过了仪器测试，则需要进行尖峰测试
                if (firstWindow) {
                    CalAndMark(jf_CountList, field_name, avg, 0);
                    CalAndMark(jf_CountList, field_name, avg, 1);
                }

                CalAndMark(jf_CountList, field_name, avg, 2);
            }

            private List<JSONObject>  setCountSize(List<JSONObject> stateCountBuffer, int size) throws Exception {
                if (stateCountBuffer.size() > size) {
                    //滑动窗口，每次向后移一位
                    stateCountBuffer = new ArrayList<>(stateCountBuffer.subList(stateCountBuffer.size() - size, stateCountBuffer.size()));
                }
                return stateCountBuffer;
            }

            /*
            卡滞测试
            duplicateDataCountState： 缓存针对特定属性所累计的重复的数据集合
            jfWindow：尖峰测试集合所在的窗口，默认大小为5
             */
            private void Kazhi_test(List<JSONObject> bufferlist, List<JSONObject> jfWindow, String field_name, boolean isFirstWindow) throws Exception {
                //完成第一次尖峰测试后，才开启卡滞测试的流程
                if (jfWindow.size() < 5) {
                    return;
                }

                //用于累计重复数据的缓存list

                //第一次进入卡滞测试，需要对第1,2条数据特殊处理。
                if (isFirstWindow) {
                    //第一条数据，直接加入buffer
                    JSONObject firstData = jfWindow.get(0);
                    bufferlist.add(firstData);

                    //第二条数据，需要和第一条数据（即buffer中的任一数据）的相应数值进行比较
                    JSONObject secondData = jfWindow.get(1);
                    Double firstValue = firstData.getJSONObject("data").getDouble(field_name);
                    Double secondValue = secondData.getJSONObject("data").getDouble(field_name);
                    //第一二条数据值不相同，说明第一条数据一定不卡滞
                    if (!secondValue.equals(firstValue)) {
                        JSONObject first_qc = firstData.getJSONObject("qc_info");
                        //数据通过仪器和尖峰测试，才进行标记
                        if (!first_qc.containsKey(field_name)) {
                            //【标记成功】
                            first_qc.put(field_name, "1,11110");
                            //      System.out.println("第一条数据标记成功");
                        }
                        //重置buffer元素，添加当前数据重新开始累计
                        bufferlist.clear();
                        bufferlist.add(secondData);
                    } else {
                        //否则累加到bufferList中
                        bufferlist.add(secondData);
                        // System.out.println("第二条数据累计到bufferList");
                    }
                }


                //默认只对尖峰测试窗口中的第n-2条数据做卡滞测试 跳出特殊阶段（程序刚运行）
                JSONObject currentData = jfWindow.get(2);

                //若buffer数组为空，直接加当前的数据进去(加入firstWindow的处理后，这个条件应该走不进来)
                if (bufferlist.isEmpty()) {
                    // System.out.println("bufferList非空，当前数据直接加入"+field_name);
                    bufferlist.add(currentData);
                } else {
                    //比较当前数据与buffer数组元素值是否相同
                    Double currentValue = currentData.getJSONObject("data").getDouble(field_name);
                    Double bufferValue = bufferlist.get(0).getJSONObject("data").getDouble(field_name);
                    if (currentValue.equals(bufferValue)) {
                        //  System.out.println("数值相同，累计到bufferList中");
                        //若值相同，则累计到buffer中
                        bufferlist.add(currentData);
                    } else {

                        //若值不相同，bufferList中的元素数小于20，则说明其中bufferList中的数据一定不卡滞，需要标记【成功】
                        // 可能的情况是 前面N条数据卡滞 但是没有达到卡滞标准  但是现在来了一条数据跟前者不一样 可以理解为前面的数据都不卡滞
                        if (bufferlist.size() < 20) {
                            for (JSONObject jsonObject : bufferlist) {
                                JSONObject kz_qc = jsonObject.getJSONObject("qc_info");
                                //数据未通过仪器测试或尖峰测试，则不进行标记
                                if (kz_qc.containsKey(field_name)) {
                                    continue;
                                }
                                //System.out.println("数据标记成功");
                                //【标记成功】
                                kz_qc.put(field_name, "1,10000");
                            }
                        }

                        //重置buffer元素，添加当前数据重新开始累计
                        bufferlist.clear();
                        bufferlist.add(currentData);
                    }
                }

                //检查buffer内数据是否累计到卡滞阈值，决定是否要标记【卡滞】
                if (bufferlist.size() == 20) {
                    //buffer里数据首次累计到阈值，需要全部数据都标记失败
                    for (JSONObject jsonObject : bufferlist) {
                        JSONObject current_qc = jsonObject.getJSONObject("qc_info");
                        //数据未通过仪器测试或尖峰测试，则不进行标记
                        if (current_qc.containsKey(field_name)) {
                            continue;
                        }
                        //【标记卡滞】
                        current_qc.put(field_name, "4,11110");
                        // System.out.println("数据标记卡滞");
                    }

                } else if (bufferlist.size() > 20) {
                    //为了避免重复标记与发送数据，20位以后的数据，标记当前数据即可
                    JSONObject lastObject = bufferlist.get(bufferlist.size() - 1);
                    JSONObject last_qc = lastObject.getJSONObject("qc_info");
                    //数据通过仪器测试和尖峰测试，才标记当前数据为卡滞
                    if (last_qc.containsKey(field_name) == false) {
                        //【标记卡滞】
                        last_qc.put(field_name, "4,11110");
                        //  System.out.println("数据标记卡滞");
                    }
                }

            }

            private double CalAvg(List<JSONObject> window, String filed_name) {
                // 计算窗口内平均值（RI）
                //存储五条数据 里面有三个值 chl  cdom  和ntu    分别计算 这三个值的尖峰 n-2 n-1 n n+1 n+2 取出来算一下
                double avg = (window.get(0).getJSONObject("data").getDouble(filed_name) +
                        window.get(1).getJSONObject("data").getDouble(filed_name) +
                        window.get(2).getJSONObject("data").getDouble(filed_name) +
                        window.get(3).getJSONObject("data").getDouble(filed_name) + +
                        window.get(4).getJSONObject("data").getDouble(filed_name) / 5.0);
                return avg;
            }

            private void CalAndMark(List<JSONObject> window, String field_name, double avg, int index) {
                JSONObject qc_info = window.get(index).getJSONObject("qc_info");
                if (qc_info.containsKey(field_name)) {
                    //如果仪器测试没通过，不进行尖峰测试
                    return;
                }

                // 获取index对应data
                double data = window.get(index).getJSONObject("data").getDouble(field_name);//
                // 异常检测：|R - RI| > N*R
                double threshold = 0.3 * data;
                boolean isAnomaly = data - avg >= threshold;
//                boolean isAnomaly = data<3;

                if (isAnomaly) {
                    //  System.out.println("尖峰测试异常   " + field_name + "值出现问题");
                    qc_info.put(field_name, "3,1110");
                }
            }


        });
        FlinkKafkaProducer<String> stringFlinkKafkaProducer1 = KafkaUtils.sendKafkaDs("dc_qc_eco", "test");
        //process.print().setParallelism(1);
//        process.map(a -> a.toString()).addSink(stringFlinkKafkaProducer1);

//      map.addSink(stringFlinkKafkaProducer1);
        process.print();
      // map.print();
        //process.print();
       // process.map(a->a.toString()).print();
        env.execute();

    }


}









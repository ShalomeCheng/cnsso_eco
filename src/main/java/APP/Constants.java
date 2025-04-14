package APP;

public class Constants {
    // 字段名称常量
    public static final String CHL_FIELD = "chl";
    public static final String CDOM_FIELD = "cdom";
    public static final String TURBIDITY_FIELD = "turbidity";
    public static final String QC_INFO = "qc_info";
    public static final String DATA = "data";
    
    // 数据转换因子
    public static final double TURBIDITY_FACTOR = 0.2419;
    public static final double CDOM_FACTOR = 0.0916;
    public static final double CHL_FACTOR = 0.0121;
    
    // 基准值
    public static final int TURBIDITY_BASE = 50;
    public static final int CDOM_BASE = 50;
    public static final int CHL_BASE = 48;
    
    // 仪器测试阈值
    public static final Double CHL_SMALL_SIZE = 50.0;
    public static final Double CHL_BIG_SIZE = 4130.0;
    public static final Double FDOM_SMALL_SIZE = 50.0;
    public static final Double FDOM_BIG_SIZE = 4130.0;
    public static final Double NTU_SMALL_SIZE = 50.0;
    public static final Double NTU_BIG_SIZE = 4130.0;
    
    // 尖峰测试阈值
    public static final double SPIKE_THRESHOLD_FACTOR = 0.3;
    
    // 窗口大小
    public static final int JIANFENG_WINDOW_SIZE = 5;
    public static final int STAGNATION_WINDOW_SIZE = 20;
    public static final int TOTAL_WINDOW_SIZE = 22;
    
    // 质量标记
    public static final String INSTRUMENT_FAILURE_MARK = "4,110";
    public static final String SPIKE_ANOMALY_MARK = "3,1110";
    public static final String STAGNATION_SUCCESS_MARK = "1,10000";
    public static final String STAGNATION_FAILURE_MARK = "4,11110";
    public static final String FIRST_WINDOW_SUCCESS_MARK = "1,11110";
    
    // Kafka 配置
    public static final String KAFKA_TOPIC_QC = "dc_qc_eco";
    public static final String KAFKA_GROUP_ID = "test";
    
    // Socket 配置
    public static final String SOCKET_HOST = "hadoop102";
    public static final int SOCKET_PORT = 7777;
} 
package cn.itcast.hotel.constants;

public class HotelMqConstants {
    // 交换机
    public static final String EXCHANGE_NAME = "hotel.topic";
    // 监听新增和修改的队列
    public static final String INSERT_QUEUE_NAME = "hotel.insert.queue";
    // 监听删除的队列
    public static final String DELETE_QUEUE_NAME = "hotel.delete.queue";
    // 新增或修改的 RoutingKey
    public static final String INSERT_KEY = "hotel.insert";
    // 删除的 RoutingKey
    public static final String DELETE_KEY = "hotel.delete";
}

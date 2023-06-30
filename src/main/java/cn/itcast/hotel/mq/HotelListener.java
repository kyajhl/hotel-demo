package cn.itcast.hotel.mq;

import cn.itcast.hotel.constants.HotelMqConstants;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author KeFan
 * @date 2023/6/30
 * @time 18:25
 */

@Component
public class HotelListener {

    @Autowired
    private IHotelService hotelService;

    /*
        监听酒店新增或修改的业务
     */
    @RabbitListener(queues = {HotelMqConstants.INSERT_QUEUE_NAME})
    public void listenHotelInsertOrUpdate(Long id) {
        hotelService.saveById(id);
    }

    /*
        监听酒店删除的业务
     */
    @RabbitListener(queues = {HotelMqConstants.DELETE_QUEUE_NAME})
    public void listenHotelDelete(Long id) {
        hotelService.deleteById(id);
    }

}

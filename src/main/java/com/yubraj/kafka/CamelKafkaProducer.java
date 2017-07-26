package com.yubraj.kafka;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yubrajpokharel on 7/26/17.
 */
public class CamelKafkaProducer {
    public static void main(String[] args) {
        final CamelContext context = new DefaultCamelContext();

        try {
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    KafkaComponent kafkaComponent = new KafkaComponent();
                    kafkaComponent.setBrokers("localhost:9092");
                    context.addComponent("kafka", kafkaComponent);

                    from("direct:pushtoTopic").routeId("DirectToKafka")
                            .to("kafka:my-first-topic").log("${headers}");
                }
            });

            ProducerTemplate template = context.createProducerTemplate();
            context.start();

            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put(KafkaConstants.PARTITION_KEY, 0);
            headers.put(KafkaConstants.KEY, "1");

            for(int i=0;i<=5;i++){
                template.sendBodyAndHeaders("direct:pushtoTopic"," Hi Hello " + i, headers);
            }
            Thread.sleep(5 * 60 * 1000);

            context.stop();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

package com.yubraj.kafka;

import jdk.nashorn.internal.runtime.ECMAException;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

/**
 * Created by yubrajpokharel on 7/16/17.
 */
public class CamelKafkaClient {
    public static void main(String[] args) throws Exception {
        CamelContext camelContext = new DefaultCamelContext();

        try {
            camelContext.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("kafka:my-first-topic?brokers=localhost:9092"
                            + "&consumersCount=1"
                            + "&seekTo=beginning"
                            + "&groupId=group1")
                            .routeId("FromKafka")
                            .log("${body}");
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        camelContext.start();
        Thread.sleep(5 * 60 * 1000);
        camelContext.stop();

    }
}

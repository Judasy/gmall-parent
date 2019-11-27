package com.wang.gmalllogger.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wang.gmall.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;


@RestController
@Slf4j
public class Demo1Controller {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    public String log(@RequestParam("logString") String logString){
        //补时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        //落盘日文件
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);
        System.out.println(logString);
        //发送到kafka
        if (jsonObject.getString("type").equals("startup")) {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "success";
    }

}

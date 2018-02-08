package org.alterac.controller;

import com.alibaba.fastjson.JSONObject;
import org.alterac.service.SampleService;
import org.apache.catalina.connector.RequestFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class SampleController {

    @Autowired
    private SampleService sampleService;

    @RequestMapping("/")
    public JSONObject index(){
        JSONObject object = new JSONObject();
        object.put("time",new Date());
        return object;
    }

    @RequestMapping("/printDate")
    public JSONObject printDate(RequestFacade requestFacade){
        sampleService.printDate();
        JSONObject object = new JSONObject();
        object.put("state","success");
        return object;
    }

    @RequestMapping("/lock")
    public JSONObject lock(){
        JSONObject object = new JSONObject();
        object.put("state",sampleService.lock());
        return object;
    }

    @RequestMapping("/unlock")
    public JSONObject unLock(){
        JSONObject object = new JSONObject();
        object.put("state",sampleService.releaseLock());
        return object;
    }

    @RequestMapping("/semaphore")
    public JSONObject semaphore(){
        JSONObject object = new JSONObject();
        object.put("state",sampleService.getSemaphore());
        return object;
    }
}

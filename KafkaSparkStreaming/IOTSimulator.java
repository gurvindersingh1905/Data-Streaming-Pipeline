package KafkaSparkStreaming;

import Model.Data;
import Model.IOTModel;
import Model.Location;

public class IOTSimulator {
    public static IOTModel createIOTSimulation(String deviceId){
        Location location = new Location();
        location.setLatitude(Double.toString(52+Math.random()*(54-52+1)));
        location.setLongitude(Double.toString(10+Math.random()*(12-10+1)));

        Data data = new Data();
        data.setDeviceId(deviceId);
        data.setLocation(location);
        data.setTemperature((int)Math.floor(10+Math.random()*(18-10+1)));
        data.setTime(Long.toString(System.currentTimeMillis()));

        IOTModel iot = new IOTModel();
        iot.setData(data);

        return iot;
    }
}

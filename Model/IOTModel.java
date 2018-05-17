package Model;

public class IOTModel{
    public Data data;

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "IOTModel{" +
                "\ndata=" + data +
                "\ndeviceId=" + data.getDeviceId() +
                "\nTime=" + data.getTime() +
                "\nTemperature=" + data.getTemperature() +
                '}';
    }
}

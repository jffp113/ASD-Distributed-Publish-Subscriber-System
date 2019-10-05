package LogsProcess;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class calculates times about process sending and receiving from logs
 */
public class LogProcess {

    private static Map<String, List<Double>> messageTimes;
    private static final String SENDER_TIME_REGEX = "./.*.txt:Publishing in topic all: m(.*) time=(.*)";
    private static final String RECEIVE_TIME_REGEX = "./.*.txt:Process .* received event at (.*): Topic: all Message: m(.*) time=.*";


    private static double media = 0;
    private static double max = Integer.MIN_VALUE;
    private static double min = Integer.MAX_VALUE;
    private static double desvio = 0;
    private static int numberOfTimes = 0;

    public static void main(String[] args) throws Exception {
        messageTimes = new HashMap<>(20000);
        sendTimeProcess("./automatedClientOutput/published.txt");
        receiveTimeProcess("./automatedClientOutput/received.txt");

        calculateTimeMean();
        desvio();
        System.out.printf("Media=%f Desvio=%f Min=%f Max=%f%n",media,desvio,min,max);
    }
    private static void calculateTimeMean(){

        int deltaT = 0;

        for(String m : messageTimes.keySet()){
            double firstTime = 0;
            for(double a : messageTimes.get(m)){
                if(firstTime == 0){
                    firstTime = a;
                }
                else{
                    numberOfTimes++;
                    double tmp = (a - firstTime);
                    //System.out.print(tmp +",");
                    deltaT += tmp;

                    if(tmp > max)
                        max = tmp;
                    if(tmp < min)
                        min = tmp;

                }
            }
        }

         media = deltaT/numberOfTimes;
    }


    private static void desvio(){
        double desvioTmp = 0;
        for(String m : messageTimes.keySet()){
            double firstTime = 0;
            for(double a : messageTimes.get(m)){
                if(firstTime == 0){
                    firstTime = a;
                }
                else{
                    double tmp = (a - firstTime);
                    double sum2 = Math.pow((tmp-media),2);
                    desvioTmp += sum2;

                }
            }
        }

        desvioTmp = (desvioTmp)/numberOfTimes;
        desvio = Math.sqrt(desvioTmp);
    }

    private static final Pattern senderPattern = Pattern.compile(SENDER_TIME_REGEX);
    private static void sendTimeProcess(String fileName) throws IOException {
        File file = new File(fileName);

        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))){
            while(true){
                try{
                    String logLine = reader.readLine();
                    if(logLine == null)
                        break;
                    Matcher m = senderPattern.matcher(logLine);
                    if(m.matches()){
                        String messageId = m.group(1);
                        double time = Double.parseDouble(m.group(2));
                        List<Double> l = new LinkedList<>();
                        l.add(time);
                        messageTimes.put(messageId,l);
                    }
                }catch (IOException e){
                    return;
                }
            }
        }
    }

    private static final Pattern receivePattern = Pattern.compile(RECEIVE_TIME_REGEX);
    private static void receiveTimeProcess(String fileName) throws IOException {
        File file = new File(fileName);

        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))){
            while(true){
                try{
                    String logLine = reader.readLine();
                    if(logLine == null)
                        break;
                    Matcher m = receivePattern.matcher(logLine);
                    if(m.matches()){
                        String messageId = m.group(2);
                        Double time = Double.parseDouble(m.group(1));
                        messageTimes.get(messageId).add(time);
                    }
                }catch (IOException e){
                    return;
                }
            }
        }
    }
}

package client;

public class AutomatedClient {


    public static void main(String[] args) throws Exception{

        if(args[0].equals("publisher")){
            publisherAuto(args);
        }else{
            subscriberAuto(args);
        }
    }


    public static void publisherAuto(String[] args) throws Exception{
        Client c = new Client(args);
    }

    public static void subscriberAuto(String[] args) throws Exception{
        Client c = new Client(args);
    }
}

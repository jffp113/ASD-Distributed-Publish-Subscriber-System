package client;


import java.util.Scanner;

public class InteractiveClient {
    public static final String EXIT = "exit";
    public static final String SPACE = " ";
    public static final String SUBSCRIBE = "subscribe";
    public static final String UNSUBSCRIBE = "unsubscribe";
    public static final String PUBLISH = "publish";
    public static final String HELP = "help";
    public static String EMPTY_STRING = "";

    public static void main(String[] args) throws Exception{
        Client c = new Client(args);
        Scanner in = new Scanner(System.in);
        String input = EMPTY_STRING;
        while (!input.equalsIgnoreCase(EXIT)) {
            input = in.nextLine();
            String[] input_array = input.split(SPACE);
            switch(input_array[0]){
                case SUBSCRIBE:
                    c.subscribe(input_array[1]);
                    break;
                case UNSUBSCRIBE:
                    c.unsubscribe(input_array[1]);
                    break;
                case PUBLISH:
                    c.publish(input_array[1],input_array[2]);
                    break;
                case HELP:
                    System.out.println("HELP SENPAI!");
                    break;
            }

        }
    }
}

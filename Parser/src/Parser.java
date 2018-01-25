import Helpers.Constants;
import Helpers.ParseCSV;
import Helpers.Strings;
import java.util.Scanner;

public class Parser {
    
    public static ParseCSV csvParser;

    public static void main(String[] args) {
        System.out.println(Strings.app_welcome);
        System.out.println(Strings.csv_choice);
        Scanner sc = new Scanner(System.in);
        try {
            int c = sc.nextInt();
            switch(c) {
                case 1 : {
                    System.out.println(Strings.please_wait);
                    csvParser = new ParseCSV(Constants.DEFAULT_CSV_FILE);
                    break;
                }
            }
        } catch (Exception e) {
            
        }
        
//        System.out.println(Strings.print_coloumn_request);
//        try {
//            String c = sc.next();
//            switch(c) {
//                case "y" : {
//                    System.out.println(Strings.print_coloumn);
//                    String coloumn_name = sc.next();
//                    csvParser.viewColoumn(coloumn_name, false);
//                    break;
//                }
//            }
//        } catch (Exception e) {
//            
//        }
    }
    
}

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpParameter {


    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;
        long startTime, endTime;


        /**
         TIANYUAN
             bucket_width=2
             r = 10
         WANHUA
             bucket_width=2
             r = 10
         JINFENG
             bucket_width=5
             r = 20
         TAO
             bucket_width=5
             r=15
         GAS
             bucket_width=0.1
             r=3
         PAMAP2
             bucket_width=0.1
             r = 0.4
         */

        // TODO: modify here
        String[] datasetList = {"tianyuan", "wanhua", "jinfeng", "tao", "gas", "PAMAP2"};
        String curDataset = datasetList[2];
        String method = "dodds";

        Map<String, Integer> map = new HashMap<>();
        map.put("jinfeng", 1000000);
        map.put("tianyuan", 1000002);
        map.put("PAMAP2", 299999);
        map.put("wanhua", 1000000);
        map.put("tao", 575468);
        map.put("gas", 928990);

        int size = 0;
        double defaultR = 0;
        int defaultK = 50, defaultW = 20, defaultS = 10;
        double[] rList = new double[10];
        int[] kList = new int[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        int[] wList = new int[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        int[] sList = new int[] {1, 2, 4, 5, 10, 20};
        double gamma  = 0 ;

        switch (curDataset){
            case "wanhua":
            case "tianyuan":
                size = 999600;
                rList = new double[] {6,7,8,9,10,11,12,13,14,15};
                defaultR = 10;
                break;
            case "jinfeng":
                size = 999600;
                rList = new double[] {12,14,16,18,20,22,24,26,28,30};
                defaultR = 20;
                gamma = 5;
                break;
            case "PAMAP2":
                size = 299400;
                rList = new double[] {0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.55,0.6};
                defaultR = 0.4;
                break;
            case "tao":
                size = 575000;
                rList = new double[] {11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
                defaultR = 15;
                break;
            case "gas":
                size = 928000;
                rList = new double[] {1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5, 6};
                defaultR = 3;
                break;
            default:
                break;
        }

        LogWriter lw;
        Random random = new Random();
        int iter = 1000;

//        lw = new LogWriter("./result/ExpR/" + curDataset + "_res.dat");
//        lw.open();
//        lw.log("Size\t" + method + "\n");
//        for (double r: rList) {
//            startTime = System.currentTimeMillis();
//            for (int i = 0; i <iter; i++) {
//                long minTime = random.nextInt(map.get(curDataset) - size) * 1000L;
//                long maxTime = minTime + size * 1000L;
//                String sql = "select " +method +"(s9, " +
//                        "'r'='" + r + "', " +
//                        "'k'='" + defaultK + "', " +
//                        "'w'='" + defaultW  + "', " +
//                        "'s'='" + defaultS + "') " +
//                        "from root." + curDataset + ".d0 " +
//                        "where time>=" + minTime + " and time<" + maxTime;;
//                System.out.println(sql);
//                result = session.executeQueryStatement(sql);
//            }
//            endTime = System.currentTimeMillis();
//            StringBuilder str = new StringBuilder();
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
//            lw.log(str);
//        }
//        lw.close();
//
        lw = new LogWriter("./result/ExpK/" + curDataset + "_res.dat");
        lw.open();
        lw.log("Size\t" + method + "\n");
        for (int k: kList){
            startTime = System.currentTimeMillis();
            for (int i = 0; i < iter; i++) {
                long minTime = random.nextInt(map.get(curDataset) - size) * 1000L;
                long maxTime = minTime + size * 1000L;
                String sql = "select dodds(s9, " +
                        "'r'='" + defaultR + "', " +
                        "'k'='" + k + "', " +
                        "'w'='" + defaultW + "', " +
                        "'s'='" + defaultS + "', " +
                        "'d'='" + 60 + "', " +
                        "'g'='" + gamma + "', " +
                        "'f'='" + 1 + "', " +
                        "'b'='true') " +
                        "from root." + curDataset + ".d0 ";
                System.out.println(sql);
                result = session.executeQueryStatement(sql);
            }
            endTime = System.currentTimeMillis();
            StringBuilder str = new StringBuilder();
            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
            lw.log(str);
        }
        lw.close();
//
//        lw = new LogWriter("./result/ExpW/" + curDataset + "_res.dat");
//        lw.open();
//        lw.log("Size\t" + method + "\n");
//        for (int w: wList){
//            startTime = System.currentTimeMillis();
//            for (int i = 0; i < iter; i++) {
//                long minTime = random.nextInt(map.get(curDataset) - size) * 1000L;
//                long maxTime = minTime + size * 1000L;
//                String sql = "select " +method +"(s9, " +
//                        "'r'='" + defaultR + "', " +
//                        "'k'='" + defaultK + "', " +
//                        "'w'='" + w  + "', " +
//                        "'s'='" + defaultS   + "') " +
//                        "from root." + curDataset + ".d0 " +
//                        "where time>=" + minTime + " and time<" + maxTime;
//                System.out.println(sql);
//                result = session.executeQueryStatement(sql);
//            }
//            endTime = System.currentTimeMillis();
//            StringBuilder str = new StringBuilder();
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
//            lw.log(str);
//        }
//        lw.close();

//        lw = new LogWriter("./result/ExpS/" + curDataset + "_res.dat");
//        lw.open();
//        lw.log("Size\t" + method + "\n");
//        for (int s: sList){
//            startTime = System.currentTimeMillis();
//            for (int i = 0; i < iter; i++) {
//                long minTime = random.nextInt(map.get(curDataset) - size) * 1000L;
//                long maxTime = minTime + size * 1000L;
//                String sql = "select " +method +"(s9, " +
//                        "'r'='" + defaultR + "', " +
//                        "'k'='" + defaultK + "', " +
//                        "'w'='" + defaultW + "', " +
//                        "'s'='" + s + "') " +
//                        "from root." + curDataset + ".d0 " +
//                        "where time>=" + minTime + " and time<" + maxTime;
//                System.out.println(sql);
//                result = session.executeQueryStatement(sql);
//            }
//            endTime = System.currentTimeMillis();
//            StringBuilder str = new StringBuilder();
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
//            lw.log(str);
//        }
//        lw.close();
    }
}

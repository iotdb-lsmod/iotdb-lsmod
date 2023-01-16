import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpOverlap {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;
        long startTime, endTime;

        /**
         TIANYUAN
         bucket_width=2
         k = 20, r = 10, w = 20 min, s = 10 min
         WANHUA
         bucket_width=2
         k = 50, r = 10, w = 20 min, s = 10 min
         JINFENG
         bucket_width=5
         k = 50, r = 20, w = 20 min, s = 10 min
         PAMAP2
         bucket_width=0.1
         k = 50, r = 0.4, w = 20 min, s = 10 min
         TAO
         bucket_width= 5
         k = 200, r = 20, w = 20 min, s = 10 min
         GAS
         bucket_width=0.1
         k = 50, r = 1.5, w = 20min, s = 10 min
         */
        // TODO: modify here
        String[] datasetList = {"tianyuan", "wanhua", "jinfeng", "tao", "gas", "PAMAP2"};
        String curDataset = datasetList[5];

        Map<String, Integer> map = new HashMap<>();
        map.put("jinfeng", 1000000);
        map.put("tianyuan", 1000002);
        map.put("PAMAP2", 299999);
        map.put("wanhua", 1000000);
        map.put("tao", 575468);
        map.put("gas", 928990);

        int delta = 60;
        boolean b;
        int k = 50;
        int w = 1200 / delta, s = 600 / delta;
        boolean ifTimeFilter = false;
        int iterNum = 20;
//        int[] fList = new int[]{1, 2, 3};
        int[] fList = new int[]{1, 2, 3, 4, 5, 6};
        double r = 0;
        int size = 0;
        double gamma  = 0 ; // bucket width
        switch (curDataset) {
            case "wanhua":
            case "tianyuan":
                size = 999600;
                r = 10;
                gamma = 2;
                break;
            case "jinfeng":
                size = 999600;
                r = 20;
                gamma = 5;
                break;
            case "PAMAP2":
                size = 299400;
                r = 0.4;
                gamma = 0.1;
                break;
            case "tao":
                size = 575000;
                r = 15;
                gamma = 5;
                break;
            case "gas":
                size = 928000;
                r = 3;
                gamma = 0.1;
                break;
            default:
                break;
        }


        LogWriter lw = new LogWriter("./result/ExpOverlap/" + curDataset + "_res.dat");
        lw.open();
//        lw.log("Size\tLSMOD(true)\tLSMOD(false)\n");

        Random random = new Random();

        for (int f : fList) {
            StringBuilder str = new StringBuilder();
            str.append(f + "\t");
            /**
             Without Bounds
             */
            b = false;
            for (int i = 0; i < iterNum; i++) {
                int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
                int maxTime = minTime + size * 1000;
                String sql = "select dodds(s9, " +
                        "'r'='" + r + "', " +
                        "'k'='" + k + "', " +
                        "'w'='" + w + "', " +
                        "'s'='" + s + "', " +
                        "'d'='" + delta + "', " +
                        "'g'='" + gamma + "', " +
                        "'f'='" + f + "', " +
                        "'b'='" + Boolean.toString(b) + "') " +
                        "from root." + curDataset + ".d0 ";
                if (ifTimeFilter)
                    sql += "where time>=" + minTime + " and time<" + maxTime;
                result = session.executeQueryStatement(sql);
            }

            startTime = System.currentTimeMillis();
            for (int i = 0; i < iterNum; i++) {
                int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
                int maxTime = minTime + size * 1000;
                String sql = "select dodds(s9, " +
                        "'r'='" + r + "', " +
                        "'k'='" + k + "', " +
                        "'w'='" + w + "', " +
                        "'s'='" + s + "', " +
                        "'d'='" + delta + "', " +
                        "'g'='" + gamma + "', " +
                        "'f'='" + f + "', " +
                        "'b'='" + Boolean.toString(b) + "') " +
                        "from root." + curDataset + ".d0 ";
                if (ifTimeFilter)
                    sql += "where time>=" + minTime + " and time<" + maxTime;
                System.out.println(sql);
                result = session.executeQueryStatement(sql);
            }
            endTime = System.currentTimeMillis();
            String resultNoBounds = String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum);
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");


            /**
             With Bounds
             */
            b = true;
            for (int i = 0; i < iterNum; i++) {
                int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
                int maxTime = minTime + size * 1000;
                String sql = "select dodds(s9, " +
                        "'r'='" + r + "', " +
                        "'k'='" + k + "', " +
                        "'w'='" + w + "', " +
                        "'s'='" + s + "', " +
                        "'d'='" + delta + "', " +
                        "'g'='" + gamma + "', " +
                        "'f'='" + f + "', " +
                        "'b'='" + Boolean.toString(b) + "') " +
                        "from root." + curDataset + ".d0 ";
                if (ifTimeFilter)
                    sql += "where time>=" + minTime + " and time<" + maxTime;
                result = session.executeQueryStatement(sql);
            }

            startTime = System.currentTimeMillis();
            for (int i = 0; i < iterNum; i++) {
                int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
                int maxTime = minTime + size * 1000;
                String sql = "select dodds(s9, " +
                        "'r'='" + r + "', " +
                        "'k'='" + k + "', " +
                        "'w'='" + w + "', " +
                        "'s'='" + s + "', " +
                        "'d'='" + delta + "', " +
                        "'g'='" + gamma + "', " +
                        "'f'='" + f + "', " +
                        "'b'='" + Boolean.toString(b) + "') " +
                        "from root." + curDataset + ".d0 ";
                if (ifTimeFilter)
                    sql += "where time>=" + minTime + " and time<" + maxTime;
                System.out.println(sql);
                result = session.executeQueryStatement(sql);
            }
            endTime = System.currentTimeMillis();
            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\t" + resultNoBounds + "\n");
            lw.log(str);

        }
    }
}

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpDelta {
//    public static String[] datasetList = {"jinfeng", "tao"};
//    public static String[] datasetList = { "tianyuan", "gas"};
    public static String[] datasetList = {"PAMAP2", "wanhua"};


    public static Map<String, Double> dataset2r = new HashMap<>();
    public static Map<String, Double> dataset2g = new HashMap<>();
    public static Map<String, Integer> dataset2size = new HashMap<>();

    public static int d = 600 * 1_000;
    public static double r;
    public static int k = 50, w = 20 * 60_000, s = 10 * 60_000;

    public static String method = "dodds";
    public static int iter = 5;


    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        dataset2r.put("tianyuan", 10.0);
        dataset2r.put("wanhua", 10.0);
        dataset2r.put("jinfeng", 20.0);
        dataset2r.put("PAMAP2", 0.4);
        dataset2r.put("tao", 15.0);
        dataset2r.put("gas", 3.0);

        dataset2g.put("tianyuan", 2.0);
        dataset2g.put("wanhua", 2.0);
        dataset2g.put("jinfeng", 5.0);
        dataset2g.put("PAMAP2", 0.1);
        dataset2g.put("tao", 5.0);
        dataset2g.put("gas", 0.1);

        dataset2size.put("tianyuan", 1_000_000);
        dataset2size.put("wanhua", 1_000_000);
        dataset2size.put("jinfeng", 1_000_000);
        dataset2size.put("PAMAP2", 299_998);
        dataset2size.put("tao", 575_498);
        dataset2size.put("gas", 928_990);

        test();
    }

    public static void test() throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;

        long startTime, endTime;
        for (String curDataset: datasetList) {
            int size = dataset2size.get(curDataset);
            double r = dataset2r.get(curDataset);
            double g = dataset2g.get(curDataset);
            LogWriter lw = new LogWriter("./result/" + method + "/ExpDelta/" + curDataset + "_res.dat");

            lw.open();
            lw.log("=======================\n");
            lw.log("Delta\tIfWithBound\t\t" + method + "\n");
            Random random = new Random();

            for (String b: new String[] {"False", "True"}) {
                startTime = System.currentTimeMillis();
                for (int j = 0; j < iter; j++) {
                    long minTime = random.nextInt(100) * 1000L;
                    long maxTime = minTime + (size - 100) * 1000L;
                    String sql = "select " + method + "(s9, " +
                            "'r'='" + r + "', " +
                            "'k'='" + k + "', " +
                            "'w'='" + w + "', " +
                            "'s'='" + s + "', " +
                            "'g'='" + g + "', " +
                            "'d'='" + d + "', " +
                            "'b'='" + b + "') " +
                            "from root." + curDataset + ".d0 " +
                            "where time >=" + minTime + " and time <=" + maxTime;
                    System.out.println(sql);
                    result = session.executeQueryStatement(sql);
                }
                endTime = System.currentTimeMillis();
                StringBuilder str = new StringBuilder();
                str.append(d + "\t" + b + "\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
                lw.log(str);
            }
            lw.close();
        }
    }
}

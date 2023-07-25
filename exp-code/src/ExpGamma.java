import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpGamma {
        public static String[] datasetList = {"jinfeng"};
//    public static String[] datasetList = {"gas"};
//    public static String[] datasetList = {"tianyuan"};
//    public static String[] datasetList = {"tao"};
//    public static String[] datasetList = {"PAMAP2"};
//    public static String[] datasetList = {"wanhua"};

    public static Map<String, Double> dataset2r = new HashMap<>();
    public static Map<String, Integer> dataset2size = new HashMap<>();
    static double[] bucketWidthList1 = new double[] {0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5};
    static double[] bucketWidthList2 = new double[] {0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1};
    static double[] bucketWidthList3 = new double[] {0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2};

    static Map<String, double[]> dataset2BucketList = new HashMap<>();


    public static int d = 60 * 1_000;
    public static double r;
    public static int k = 50, w = 20 * 60_000, s = 10 * 60_000;

    public static String method = "dodds";
    public static int iter = 1;


    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        dataset2r.put("tianyuan", 10.0);
        dataset2r.put("wanhua", 10.0);
        dataset2r.put("jinfeng", 20.0);
        dataset2r.put("PAMAP2", 0.4);
        dataset2r.put("tao", 15.0);
        dataset2r.put("gas", 3.0);


        dataset2size.put("tianyuan", 1_000_000);
        dataset2size.put("wanhua", 1_000_000);
        dataset2size.put("jinfeng", 1_000_000);
        dataset2size.put("PAMAP2", 299_998);
        dataset2size.put("tao", 575_498);
        dataset2size.put("gas", 928_990);

        dataset2BucketList.put("wanhua", bucketWidthList1);
        dataset2BucketList.put("jinfeng", bucketWidthList1);
        dataset2BucketList.put("tianyuan", bucketWidthList1);
        dataset2BucketList.put("tao", bucketWidthList1);
        dataset2BucketList.put("gas", bucketWidthList2);
        dataset2BucketList.put("PAMAP2", bucketWidthList3);

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
            LogWriter lw = new LogWriter("./result/" + method + "/ExpGamma/" + curDataset + "_res.dat");

            lw.open();
            lw.log("=======================\n");
            lw.log("Gamma\tFalse\tTrue\t" + method + "\n");
            Random random = new Random();
            for (int i = 0; i < dataset2BucketList.get(curDataset).length; i++) {
                if (i >=2) iter = 10;
                double g = dataset2BucketList.get(curDataset)[i];
                StringBuilder str = new StringBuilder();
                str.append(g + "\t");
                for (String b : new String[]{"False", "True"}) {
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
                                "from root." + curDataset + (i+1) + ".d0 " +
                                "where time >=" + minTime + " and time <=" + maxTime;
                        System.out.println(sql);
                        result = session.executeQueryStatement(sql);
                    }
                    endTime = System.currentTimeMillis();
                    str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\t");
                }
                str.append("\n");
                lw.log(str);
            }
            lw.close();
        }
    }
}

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.*;
import java.util.Random;

public class ExpParam {

//    public static String[] datasetList = {"gas", "jinfeng", "PAMAP2", "tao", "wanhua", "tianyuan"};
    public static String[] datasetList = {"tianyuan"};

    public static Map<String, Double> dataset2r = new HashMap<>();
    public static Map<String, double[]> dataset2rList = new HashMap<>();
    public static Map<String, Integer> dataset2size = new HashMap<>();

    public static double r;
    public static int k = 50, w = 20 * 60, s = 10*60;

    public static String method = "nets";
    public static int iter = 10;

    public static int[] kList = new int[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    public static int[] wList = new int[] {10 * 60, 20 * 60, 30 * 60, 40 * 60, 50 * 60,
            60 * 60, 70 * 60, 80 * 60, 90 * 60, 100 * 60};
    public static int[] sList = new int[] {1 * 60, 2 * 60, 4 * 60, 5 * 60, 10 * 60, 20 * 60};



    public static void main(String[] args) throws StatementExecutionException, IoTDBConnectionException {

        dataset2r.put("tianyuan", 10.0);
        dataset2r.put("wanhua", 10.0);
        dataset2r.put("jinfeng", 20.0);
        dataset2r.put("PAMAP2", 0.4);
        dataset2r.put("tao", 15.0);
        dataset2r.put("gas", 3.0);

        dataset2rList.put("tianyuan", new double[]{6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        dataset2rList.put("wanhua", new double[]{6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        dataset2rList.put("jinfeng", new double[]{12, 14, 16, 18, 20, 22, 24, 26, 28, 30});
        dataset2rList.put("PAMAP2", new double[]{0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6});
        dataset2rList.put("tao", new double[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20});
        dataset2rList.put("gas", new double[]{1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5, 6});

        dataset2size.put("tianyuan", 1_000_000);
        dataset2size.put("wanhua", 1_000_000);
        dataset2size.put("jinfeng", 1_000_000);
        dataset2size.put("PAMAP2", 299_998);
        dataset2size.put("tao", 575_498);
        dataset2size.put("gas", 928_990);

        testS();
    }

    public static void testR() throws IoTDBConnectionException, StatementExecutionException{
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;

        long startTime, endTime;

        for (String curDataset: datasetList) {
            double[] rList = dataset2rList.get(curDataset);
            int size = dataset2size.get(curDataset);
            LogWriter lw = new LogWriter("./result/"+method+"/ExpR/" + curDataset + "_res.dat");

            lw.open();
            lw.log("=======================\n");
            lw.log("R\t\t"+ method + "\n");
            Random random = new Random();

            for (double tmpR: rList) {
               startTime = System.currentTimeMillis();
                for (int d = 0; d < iter; d++) {
                    long minTime = random.nextInt(100) * 1000L;
                    long maxTime = minTime + (size - 100 )* 1000L;
                    String sql = "select " + method + "(s9, " +
                            "'r'='" + tmpR + "', " +
                            "'k'='" + k + "', " +
                            "'w'='" + w + "', " +
                            "'s'='" + s + "') " +
                            "from root." + curDataset + ".d0 " +
                            "where time >=" + minTime +" and time <=" + maxTime;
                    System.out.println(sql);
                    result = session.executeQueryStatement(sql);
                }
                endTime = System.currentTimeMillis();

                StringBuilder str = new StringBuilder();
                str.append(tmpR+ "\t\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
                lw.log(str);
            }
            lw.close();
        }
    }

    public static void testK() throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;

        long startTime, endTime;

        for (String curDataset : datasetList) {
            r = dataset2r.get(curDataset);
            int size = dataset2size.get(curDataset);
            LogWriter lw = new LogWriter("./result/"+method+"/ExpK/" + curDataset + "_res.dat");
            lw.open();
            lw.log("=======================\n");
            lw.log("K\t\t"+ method + "\n");
            Random random = new Random();

            for (int tmpK: kList) {
                startTime = System.currentTimeMillis();
                for (int d = 0; d < iter; d++) {
                    long minTime = random.nextInt(100) * 1000L;
                    long maxTime = minTime + (size - 100 )* 1000L;
                    String sql = "select " + method + "(s9, " +
                            "'r'='" + r + "', " +
                            "'k'='" + tmpK + "', " +
                            "'w'='" + w + "', " +
                            "'s'='" + s + "') " +
                            "from root." + curDataset + ".d0 " +
                            "where time >=" + minTime +" and time <=" + maxTime;
                    System.out.println(sql);
                    result = session.executeQueryStatement(sql);
                }
                endTime = System.currentTimeMillis();

                StringBuilder str = new StringBuilder();
                str.append(tmpK+ "\t\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
                lw.log(str);
            }
            lw.close();
        }
    }

    public static void testW() throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;

        long startTime, endTime;

        for (String curDataset : datasetList) {
            r = dataset2r.get(curDataset);
            int size = dataset2size.get(curDataset);
            LogWriter lw = new LogWriter("./result/"+method+"/ExpW/" + curDataset + "_res.dat");
            lw.open();
            lw.log("=======================\n");
            lw.log("W\t\t"+ method + "\n");
            Random random = new Random();

            for (int tmpW: wList) {
                startTime = System.currentTimeMillis();
                for (int d = 0; d < iter; d++) {
                    long minTime = random.nextInt(100) * 1000L;
                    long maxTime = minTime + (size - 100 )* 1000L;
                    String sql = "select " + method + "(s9, " +
                            "'r'='" + r + "', " +
                            "'k'='" + k + "', " +
                            "'w'='" + tmpW + "', " +
                            "'s'='" + s + "') " +
                            "from root." + curDataset + ".d0 " +
                            "where time >=" + minTime +" and time <=" + maxTime;
                    System.out.println(sql);
                    result = session.executeQueryStatement(sql);
                }
                endTime = System.currentTimeMillis();

                StringBuilder str = new StringBuilder();
                str.append(tmpW+ "\t\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
                lw.log(str);
            }
            lw.close();
        }
    }

    public static void testS() throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;

        long startTime, endTime;

        for (String curDataset : datasetList) {
            r = dataset2r.get(curDataset);
            int size = dataset2size.get(curDataset);
            LogWriter lw = new LogWriter("./result/"+method+"/ExpS/" + curDataset + "_res.dat");
            lw.open();
            lw.log("=======================\n");
            lw.log("S\t\t"+ method + "\n");
            Random random = new Random();

            for (int tmpS: sList) {
                startTime = System.currentTimeMillis();
                for (int d = 0; d < iter; d++) {
                    long minTime = random.nextInt(100) * 1000L;
                    long maxTime = minTime + (size - 100 )* 1000L;
                    String sql = "select " + method + "(s9, " +
                            "'r'='" + r + "', " +
                            "'k'='" + k + "', " +
                            "'w'='" + w + "', " +
                            "'s'='" + tmpS + "') " +
                            "from root." + curDataset + ".d0 " +
                            "where time >=" + minTime +" and time <=" + maxTime;
                    System.out.println(sql);
                    result = session.executeQueryStatement(sql);
                }
                endTime = System.currentTimeMillis();

                StringBuilder str = new StringBuilder();
                str.append(tmpS+ "\t\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
                lw.log(str);
            }
            lw.close();
        }
    }
}
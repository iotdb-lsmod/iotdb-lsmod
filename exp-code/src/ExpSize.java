import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.*;

public class ExpSize {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;
        long startTime, endTime;

//        String[] datasetList = {"PAMAP2", "tao", "gas"};
        String[] datasetList = {"tianyuan", "jinfeng", "wanhua"};

        Map<String, Double> dataset2r = new HashMap<>();
        dataset2r.put("tianyuan", 10.0);
        dataset2r.put("wanhua", 10.0);
        dataset2r.put("jinfeng", 20.0);
        dataset2r.put("PAMAP2", 0.4);
        dataset2r.put("tao", 15.0);
        dataset2r.put("gas", 3.0);

        Map<String, int[]> dataset2list = new HashMap<>();
        dataset2list.put("tianyuan", new int[]{100_000, 200_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000,
                20_000_000, 50_000_000, 100_000_000});
        dataset2list.put("wanhua", new int[]{100_000, 200_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000,
                20_000_000, 50_000_000, 100_000_000});
        dataset2list.put("jinfeng", new int[]{100_000, 200_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000,
                20_000_000, 50_000_000, 100_000_000});
        dataset2list.put("PAMAP2", new int[]{30_000, 60_000, 90_000, 120_000, 150_000, 180_000, 210_000, 240_000,
                270_000, 300_000});
        dataset2list.put("tao", new int[]{60_000, 120_000, 180_000, 240_000, 300_000, 360_000, 420_000, 480_000,
                540_000, 600_000});
        dataset2list.put("gas", new int[]{90_000, 180_000, 270_000, 360_000, 450_000, 540_000, 630_000, 720_000,
                810_000, 900_000});

        double r;
        int k = 50, w = 20 * 60, s = 10*60;

        String method = "cpod";
        int iter = 4;

        for (String curDataset: datasetList) {
            r = dataset2r.get(curDataset);
            LogWriter lw = new LogWriter("./result/new/ExpSize/" + curDataset + "_res.dat");
            lw.open();
            lw.log("=======================\n");
            lw.log("Size\t"+ method + "\n");

            Random random = new Random();

            for (int j = 9; j >= 0; j--) {
                int allNum = dataset2list.get(curDataset)[j];

                startTime = System.currentTimeMillis();
                for (int d = 0; d < iter; d++) {
                    long minTime = random.nextInt(100) * 1000L;
                    long maxTime = minTime + (long) (allNum - 100) * 1000L;
                    String sql = "select " + method + "(s10, " +
                            "'r'='" + r + "', " +
                            "'k'='" + k + "', " +
                            "'w'='" + w + "', " +
                            "'s'='" + s + "') " +
                            "from root." + curDataset + ".d0 " +
                            "where time>=" + minTime + " and time<" + maxTime;
                    System.out.println(sql);
                    result = session.executeQueryStatement(sql);
                }

                endTime = System.currentTimeMillis();

                StringBuilder str = new StringBuilder();
                str.append(allNum + "\t");
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iter) + "\n");
                lw.log(str);
            }
            lw.close();
        }
    }
}

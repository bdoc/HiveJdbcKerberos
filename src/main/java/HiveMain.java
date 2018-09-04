import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;

public class HiveMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        System.out.println("Usage: keyUser,keyPath,jdbcUrl,sql");

        String user = "dw";
        String path = "/etc/security/keytabs/dw.keytab";
        String url = "jdbc:hive2://plat.main.ambari.bjb.003.hadoop:10000/default;principal=hive/plat.main.ambari.bjb.003.hadoop@LG.COM";
        String sql = "show databases";

        if (args.length >= 4) {
            user = args[0];
            path = args[1];
            url = args[2];
            sql = args[3];
        }

        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "Kerberos");

        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab(user, path);

        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection connection = DriverManager.getConnection(url);

        Statement statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }

        connection.close();
    }
}

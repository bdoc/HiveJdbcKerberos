import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;

public class HiveMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        System.out.println("Usage: keyUser,keyPath,jdbcUrl,sql");
        // without krb5.conf
        System.setProperty("java.security.krb5.realm", "REALM.COM");
        System.setProperty("java.security.krb5.kdc", "kdc-host");

        String user = "user";
        String path = "user.keytab";
        String url = "jdbc:hive2://hs2-host:port/default;principal=hive/hs2-host@REALM.COM";
        String sql = "show databases";

        if (args.length >= 4) {
            user = args[0];
            path = args[1];
            url = args[2];
            sql = args[3];
        }

        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "kerberos");

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

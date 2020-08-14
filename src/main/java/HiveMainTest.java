import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

public class HiveMainTest {

    @Test
    public void main() throws SQLException, IOException, ClassNotFoundException {
        // without krb5.conf
        System.setProperty("java.security.krb5.realm", "T2.IGETKDC.COM");
        System.setProperty("java.security.krb5.kdc", "node06.test");

        String user = "dwadmin";
        String path = System.getenv("HOME").concat("/Documents/krb5/dwadmin.keytab-t2");
        String url = "jdbc:hive2://node06.test:10025/default;principal=hive/node06.test@T2.IGETKDC.COM";
        String sql = "show databases";
        String[] args = new String[4];
        args[0] = user;
        args[1] = path;
        args[2] = url;
        args[3] = sql;
        HiveMain.main(args);
    }
}
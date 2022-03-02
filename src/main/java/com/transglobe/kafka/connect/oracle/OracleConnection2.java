package com.transglobe.kafka.connect.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *  
 * @author Erdem Cer (erdemcer@gmail.com)
 */

public class OracleConnection2{    
    
    public Connection connect(OracleSourceConnectorConfig2 config) throws SQLException{
        return DriverManager.getConnection(
            "jdbc:oracle:thin:@"+config.getDbHostName()+":"+config.getDbPort()+"/"+config.getDbName(),
            config.getDbUser(),
            config.getDbUserPassword());
    }
}
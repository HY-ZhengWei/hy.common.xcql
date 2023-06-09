package org.hy.common.xcql.junit;

import org.hy.common.Help;
import org.hy.common.xcql.DataSourceCQL;
import org.hy.common.xml.log.Logger;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;





/**
 * 图数据库会话的测试单元
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-05-24
 * @version     v1.0
 */
public class JU_DataSourceCQL
{
    private static final Logger $Logger = new Logger(JU_DataSourceCQL.class ,true);
    
    
    
    @Test
    public void test_DBCQL()
    {
        DataSourceCQL v_DSCQL = new DataSourceCQL();
        
        v_DSCQL.setUrl("neo4j://127.0.0.1:7687");
        v_DSCQL.setUsername("neo4j");
        v_DSCQL.setPassword("ZhengWei@qq.com");
        v_DSCQL.setDatabase("cdc");
        
        Session v_Session = v_DSCQL.getConnection();
        
        Result v_Result = v_Session.run("MATCH (n:`数据源`) RETURN n");
        while (v_Result.hasNext())
        {
            Record v_Record = v_Result.next();
            
            Help.print(v_Record.keys());
            
            $Logger.info(v_Record.get("n").get("xid"));
        }
        v_Session.close();
    }
    
}

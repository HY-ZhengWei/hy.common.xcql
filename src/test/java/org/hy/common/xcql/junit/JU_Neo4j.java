package org.hy.common.xcql.junit;

import java.util.List;

import org.hy.common.xml.log.Logger;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;





/**
 * Neo4j的测试单元
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-05-24
 * @version     v1.0
 */
public class JU_Neo4j
{
    private static final Logger $Logger = new Logger(JU_Neo4j.class ,true);
    
    
    
    @Test
    public void test_Neo4j_001()
    {
        Driver        v_Driver        = GraphDatabase.driver("neo4j://127.0.0.1:7687" ,AuthTokens.basic("neo4j", "ZhengWei@qq.com"));
        SessionConfig v_SessionConfig = SessionConfig.forDatabase("cdc");
        Session       v_Session       = v_Driver.session(v_SessionConfig);
        
        Result v_Result = v_Session.run("MATCH (n:`数据源`) ,(r:`源端表`) RETURN n.id ,r.id AS id2");
        // 遍历每条记录
        while (v_Result.hasNext())
        {
            Record       v_Record = v_Result.next();
            List<String> v_RNames = v_Record.keys();
            
            // 遍历每条记录中的每个数据子集
            for (String v_RName : v_RNames)
            {
                Value            v_RData      = v_Record.get(v_RName);
                Iterable<String> v_FieldNames = v_RData.keys();
                boolean          v_IsEmpty    = true;
                
                // 遍历节点属性
                for (String v_FieldName : v_FieldNames)
                {
                    $Logger.info(v_FieldName + " = " + v_RData.get(v_FieldName ,""));
                    v_IsEmpty = false;
                }
                
                if ( v_IsEmpty )
                {
                    $Logger.info(v_RName + " = " + v_RData.asString());
                }
            }
            
            // $Logger.info(v_Record.get("n").get("xid"));
        }
        v_Session.close();
        v_Driver.close();
    }
    
    
    
    @Test
    public void test_Neo4j_002()
    {
        Driver        v_Driver        = GraphDatabase.driver("neo4j://127.0.0.1:7687" ,AuthTokens.basic("neo4j", "ZhengWei@qq.com"));
        SessionConfig v_SessionConfig = SessionConfig.forDatabase("cdc");
        Session       v_Session       = v_Driver.session(v_SessionConfig);
        
        Result v_Result          = v_Session.run("CREATE (:测试 {name: '测试01'})");
        int    v_CreateNodeCount = v_Result.consume().counters().nodesCreated();
        int    v_SetCount        = v_Result.consume().counters().propertiesSet();
        
        $Logger.info("创建节点数量：" + v_CreateNodeCount);
        $Logger.info("设置属性数量：" + v_SetCount);
        
        v_Session.close();
        v_Driver.close();
    }
    
    
    
    @Test
    public void test_Neo4j_003()
    {
        Driver        v_Driver        = GraphDatabase.driver("neo4j://127.0.0.1:7687" ,AuthTokens.basic("neo4j", "ZhengWei@qq.com"));
        SessionConfig v_SessionConfig = SessionConfig.forDatabase("cdc");
        Session       v_Session       = v_Driver.session(v_SessionConfig);
        
        Result v_Result   = v_Session.run("MATCH (n:测试) WHERE n.name = '测试01' SET n.age = 18");
        int    v_SetCount = v_Result.consume().counters().propertiesSet();
        
        $Logger.info("设置属性数量：" + v_SetCount);
        
        v_Session.close();
        v_Driver.close();
    }
    
}

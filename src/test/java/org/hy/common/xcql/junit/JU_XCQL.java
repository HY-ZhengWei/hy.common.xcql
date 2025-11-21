package org.hy.common.xcql.junit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hy.common.Date;
import org.hy.common.Help;
import org.hy.common.xcql.XCQL;
import org.hy.common.xcql.XCQLData;
import org.hy.common.xml.XJava;
import org.hy.common.xml.annotation.XType;
import org.hy.common.xml.annotation.Xjava;
import org.hy.common.xml.log.Logger;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;





/**
 * 测试单元：XCQL基本功能的测试
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-06
 * @version     v1.0
 */
@Xjava(value=XType.XML)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JU_XCQL
{
    private static Logger  $Logger = new Logger(JU_XCQL.class ,true);
    
    private static boolean $isInit = false;
    
    
    
    public JU_XCQL() throws Exception
    {
        if ( !$isInit )
        {
            $isInit = true;
            XJava.parserAnnotation(JU_XCQL.class.getName());
        }
    }
    
    
    
    /**
     * 对象为参数，写入一个节点，方式1
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @Test
    public void test_Insert_001_Object()
    {
        DataSourceConfig v_Param = new DataSourceConfig();
        v_Param.setId("111");
        v_Param.setDatabaseName("dataCenter");
        v_Param.setPort(3306);
        v_Param.setCreateTime(new Date());
        
        XCQL     v_XCQL     = (XCQL) XJava.getObject("XCQL_Insert_001");
        XCQLData v_XCQLData = v_XCQL.executeInsert(v_Param);
        
        $Logger.info(v_XCQLData.getRowCount());
    }
    
    
    
    /**
     * 对象为参数，写入一个节点，方式2
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @Test
    public void test_Insert_002_Object()
    {
        DataSourceConfig v_Param = new DataSourceConfig();
        v_Param.setId("222");
        v_Param.setDatabaseName("dataCenter222");
        v_Param.setPort(3306);
        v_Param.setCreateTime(new Date());
        
        XCQL v_XCQL  = (XCQL) XJava.getObject("XCQL_Insert_001");
        int  v_Count = v_XCQL.executeUpdate(v_Param);
        
        $Logger.info(v_Count);
    }
    
    
    
    /**
     * Map为参数，写入一个节点
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @Test
    public void test_Insert_003_Map()
    {
        Map<String ,Object> v_Param = new HashMap<String ,Object>();
        v_Param.put("id"           ,"333");
        v_Param.put("databaseName" ,"dataCenter");
        v_Param.put("port"         ,3306);
        v_Param.put("createTime"   ,new Date());
        
        XCQL v_XCQL  = (XCQL) XJava.getObject("XCQL_Insert_001");
        int  v_Count = v_XCQL.executeUpdate(v_Param);
        
        $Logger.info(v_Count);
    }
    
    
    
    /**
     * 批量写入数据（统一提交、统一回滚），方式1
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @Test
    public void test_Insert_004_Batch()
    {
        List<DataSourceConfig> v_Params = new ArrayList<DataSourceConfig>();
        for (int x=1; x<=10; x++)
        {
            DataSourceConfig v_Param = new DataSourceConfig();
            v_Param.setId("100" + x);
            v_Param.setDatabaseName("dataCenter_" + v_Param.getId());
            v_Param.setPort(3306 + x);
            v_Param.setCreateTime(new Date());
            
            v_Params.add(v_Param);
        }
        
        XCQL     v_XCQL  = (XCQL) XJava.getObject("XCQL_Insert_001");
        XCQLData v_Count = v_XCQL.executeInserts(v_Params);
        
        $Logger.info(v_Count.getRowCount());
    }
    
    
    
    /**
     * 批量写入数据（统一提交、统一回滚），方式2
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @Test
    public void test_Insert_005_Batch()
    {
        List<DataSourceConfig> v_Params = new ArrayList<DataSourceConfig>();
        for (int x=1; x<=10; x++)
        {
            DataSourceConfig v_Param = new DataSourceConfig();
            v_Param.setId("200" + x);
            v_Param.setDatabaseName("dataCenter_" + v_Param.getId());
            v_Param.setPort(3306 + x);
            v_Param.setCreateTime(new Date());
            
            v_Params.add(v_Param);
        }
        
        XCQL v_XCQL  = (XCQL) XJava.getObject("XCQL_Insert_001");
        int  v_Count = v_XCQL.executeUpdates(v_Params);
        
        $Logger.info(v_Count);
    }
    
    
    
    /**
     * 查询：返回List<Map>结构
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @Test
    @SuppressWarnings("unchecked")
    public void test_Query_001_ReturnListMap()
    {
        XCQL                      v_XCQL     = (XCQL) XJava.getObject("XCQL_Query_001_ReturnListMap");
        XCQLData                  v_XCQLData = v_XCQL.queryXCQLData();
        List<Map<String ,Object>> v_Datas    = (List<Map<String ,Object>>) v_XCQLData.getDatas();
        
        for (Map<String ,Object> v_Item : v_Datas)
        {
            Help.print(v_Item);
        }
    }
    
    
    
    /**
     * 查询：返回List<Java Bean>结构
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_Query_002_ReturnObject()
    {
        XCQL                   v_XCQL     = (XCQL) XJava.getObject("XCQL_Query_002_ReturnObject");
        XCQLData               v_XCQLData = v_XCQL.queryXCQLData();
        List<DataSourceConfig> v_Datas    = (List<DataSourceConfig>) v_XCQLData.getDatas();
        
        for (DataSourceConfig v_Item : v_Datas)
        {
            $Logger.info(v_Item.getDatabaseName());
        }
    }
    
    
    
    /**
     * 查询：带占位符的查询条件是对象的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_Query_003_WhereObject()
    {
        DataSourceConfig v_Param = new DataSourceConfig();
        v_Param.setDatabaseName("dataCenter");
        
        XCQL                   v_XCQL     = (XCQL) XJava.getObject("XCQL_Query_003_Where");
        XCQLData               v_XCQLData = v_XCQL.queryXCQLData(v_Param);
        List<DataSourceConfig> v_Datas    = (List<DataSourceConfig>) v_XCQLData.getDatas();
        
        for (DataSourceConfig v_Item : v_Datas)
        {
            $Logger.info(v_Item.getDatabaseName());
        }
    }
    
    
    
    /**
     * 查询：带占位符的查询条件是Map的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_Query_004_WhereMap()
    {
        Map<String ,Object> v_Param = new HashMap<String ,Object>();
        v_Param.put("databaseName" ,"dataCenter");
        
        XCQL                   v_XCQL     = (XCQL) XJava.getObject("XCQL_Query_003_Where");
        XCQLData               v_XCQLData = v_XCQL.queryXCQLData(v_Param);
        List<DataSourceConfig> v_Datas    = (List<DataSourceConfig>) v_XCQLData.getDatas();
        
        for (DataSourceConfig v_Item : v_Datas)
        {
            $Logger.info(v_Item.getDatabaseName());
        }
    }
    
    
    
    /**
     * 查询：分页
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_Query_004_Paging()
    {
        Map<String ,Object> v_Param = new HashMap<String ,Object>();
        v_Param.put("startIndex"   ,0);    // 从第几行分页。有效下标从0开始
        v_Param.put("pagePerCount" ,2);    // 每页显示数量
        
        XCQL                   v_XCQL       = (XCQL) XJava.getObject("XCQL_Query_004_Paging");
        XCQL                   v_XCQLPaging = XCQL.queryPaging(v_XCQL ,true);
        XCQLData               v_XCQLData   = v_XCQLPaging.queryXCQLData(v_Param);
        List<DataSourceConfig> v_Datas      = (List<DataSourceConfig>) v_XCQLData.getDatas();
        
        for (DataSourceConfig v_Item : v_Datas)
        {
            $Logger.info(v_Item.getDatabaseName());
        }
    }
    
    
    
    /**
     * 操作节点属性
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @Test
    public void test_Set_001_Where()
    {
        DataSourceConfig v_Param = new DataSourceConfig();
        v_Param.setDatabaseName("dataCenter");
        v_Param.setPort(9999);
        v_Param.setUserName("ZhengWei");
        
        XCQL     v_XCQL     = (XCQL) XJava.getObject("XCQL_Set_001_Where");
        XCQLData v_XCQLData = v_XCQL.executeInsert(v_Param);
        
        $Logger.info(v_XCQLData.getRowCount() + " - " + v_XCQLData.getColCount());
    }
    
    
    
    /**
     * 删除操作节点属性
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @Test
    public void test_Remove_001_Where()
    {
        DataSourceConfig v_Param = new DataSourceConfig();
        v_Param.setDatabaseName("dataCenter");
        
        XCQL v_XCQL  = (XCQL) XJava.getObject("XCQL_Remove_001_Where");
        int  v_Count = v_XCQL.executeUpdate(v_Param);
        
        $Logger.info(v_Count);
    }
    
    
    
    /**
     * 删除节点属性
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-24
     * @version     v1.0
     *
     */
    @Test
    public void test_Delete_001_Where()
    {
        DataSourceConfig v_Param = new DataSourceConfig();
        v_Param.setDatabaseName("dataCenter_3");
        
        XCQL v_XCQL  = (XCQL) XJava.getObject("XCQL_Delete_001_Where");
        int  v_Count = v_XCQL.executeUpdate(v_Param);
        
        $Logger.info(v_Count);
    }
    
    
    
    /**
     * 删除节点及节点中的所有属性
     * 
     * @author      ZhengWei(HY)
     * @createDate  2025-11-21
     * @version     v1.0
     *
     */
    @Test
    public void test_Delete_001_All()
    {
        DataSourceConfig v_Param = new DataSourceConfig();
        
        XCQL v_XCQL  = (XCQL) XJava.getObject("XCQL_Delete_001_All");
        int  v_Count = v_XCQL.executeUpdate(v_Param);
        
        $Logger.info(v_Count);
    }
    
}

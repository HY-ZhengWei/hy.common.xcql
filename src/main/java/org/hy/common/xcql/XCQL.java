package org.hy.common.xcql;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hy.common.AnalyseTotal;
import org.hy.common.Busway;
import org.hy.common.CycleNextList;
import org.hy.common.Date;
import org.hy.common.Help;
import org.hy.common.PartitionMap;
import org.hy.common.StaticReflect;
import org.hy.common.StringHelp;
import org.hy.common.TablePartitionBusway;
import org.hy.common.XJavaID;
import org.hy.common.xml.log.Logger;
import org.hy.common.xml.plugins.XRule;
import org.neo4j.driver.Result;





/**
 * 解释Xml文件，执行占位符CQL，再分析数据库结果集转化为Java实例对象。
 * 
 * 1. 必须是数据库连接池的。但微型应用(如手机App)，可用无连接池概念的 org.hy.common.DataSourceNoPool 代替。
 * 
 * 2. 有占位符CQL分析功能。
 *    A. 可按对象填充占位符CQL; 同时支持动态CQL，动态标识 <[ ... ]>
 *    B. 可按集合填充占位符CQL。同时支持动态CQL，动态标识 <[ ... ]>
 *    C. CQL语句生成时，对于占位符，可实现xxx.yyy.www(或getXxx.getYyy.getWww)全路径的解释。如，':shool.BeginTime'
 * 
 * 3. 对结果集输出字段转为Java实例，有过滤功能。
 *    A. 可按字段名称过滤;
 *    B. 可按字段位置过滤。
 * 
 * 4. 支持XCQL触发器
 * 
 * 5.外界有自行定制的XCQL异常处理的机制
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-06-02
 * @version     v1.0
 *              v2.0 2023-10-18  添加：是否附加触发额外参数 triggerParams
 *              v3.0 2025-11-24  优化：生成分页对象时，设置XJavaID
 *                               添加：对外提删除克隆生成的分页对象
 */
public final class XCQL extends AnalyseTotal implements Comparable<XCQL> ,XJavaID
{
    private static final Logger                                $Logger              = new Logger(XCQL.class ,true);
    
    /** CQL类型。N: 增、删、改、查的普通CQL语句  (默认值) */
    public  static final String                                $Type_NormalCQL      = "N";
    
    /** CQL类型。C：DDL、DCL、TCL创建表，创建对象等 */
    public  static final String                                $Type_Create         = "C";
    
    /** execute()方法中执行多条CQL语句的分割符 */
    public  static final String                                $Executes_Split      = ";/";
    
    /** 每个XCQL对象的执行日志。默认每个XCQL对象只保留100条日志。按 getObjectID() 分区 */
    public  static final TablePartitionBusway<String ,XCQLLog> $CQLBuswayTP         = new TablePartitionBusway<String ,XCQLLog>();
    
    /** 所有CQL执行日志，有一定的执行顺序。默认只保留5000条执行过的CQL语句 */
    public  static final Busway<XCQLLog>                       $CQLBusway           = new Busway<XCQLLog>(1000);
    
    /** CQL执行异常的日志。默认只保留9000条执行异常的CQL语句 */
    public  static final Busway<XCQLLog>                       $CQLBuswayError      = new Busway<XCQLLog>(1000);
    
    /** XCQL */
    public  static final String                                $XCQLErrors          = "XCQL-Errors";
    
    
    
    /** 触发器的额外附加参数名称：触发源的ID，即XSQL.getObjectID() */
    public  static final String                                $Trigger_SourceID    = "XT_SourceID";
    
    /** 触发器的额外附加参数名称：触发源的XJavaID */
    public  static final String                                $Trigger_SourceXID   = "XT_SourceXID";
    
    /** 触发器的额外附加参数名称：触发源的执行开始时间 */
    public  static final String                                $Trigger_StartTime   = "XT_StartTime";
    
    /** 触发器的额外附加参数名称：触发源的执行结束时间 */
    public  static final String                                $Trigger_EndTime     = "XT_EndTime";
    
    /** 触发器的额外附加参数名称：触发源的执行是否异常 */
    public  static final String                                $Trigger_IsError     = "XT_IsError";
    
    /** 触发器的额外附加参数名称：触发源的异常信息 */
    public  static final String                                $Trigger_ErrorInfo   = "XT_ErrorInfo";
    
    /** 触发器的额外附加参数名称：触发源的执行方式 */
    public  static final String                                $Trigger_ExecuteType = "XT_ExecuteType";
    
    /** 触发器的额外附加参数名称：触发源的读写行数 */
    public  static final String                                $Trigger_IORowCount  = "XT_IORowCount";
    
    
    
    /**
     * 通用分区XCQL标示记录（确保只操作一次，而不是重复执行替换操作）
     * Map.key   为数据库类型 + "_" + XCQL.getObjectID()
     * Map.value 为 XCQL
     */
    private static final Map<String ,XCQL>                     $PagingMap           = new HashMap<String ,XCQL>();
                                                               
    /** 缓存大小 */
    protected static final int                                 $BufferSize          = 4 * 1024;
    
    /** 对象池，在无XJava的环境中使用的兼容模式 */
    private static final Map<String ,Object>                   $CQLPool             = new HashMap<String ,Object>();
    
    
    
    static
    {
        $CQLBuswayTP.setDefaultWayLength(100);
        // XJava.putObject("$CQLBuswayTP"    ,$CQLBuswayTP);
        // XJava.putObject("$CQLBusway"      ,$CQLBusway);
        // XJava.putObject("$CQLBuswayError" ,$CQLBuswayError);
    }
    
    
    
    /** XJava池中对象的ID标识 */
    private String                         xjavaID;
    
    /**
     * 多个平行、平等的数据库的负载数据库集合
     * 
     * 实现多个平行、平等的数据库的负载均衡（简单级的）。
     * 目前建议只用在查询CQL上，当多个相同数据的数据库（如主备数据库），
     * 在高并发的情况下，提高整体查询速度，查询锁、查询阻塞等问题均能得到一定的解决。
     * 在高并发的情况下，突破数据库可分配的连接数量，会话数量将翻数倍（与数据库个数有正相关的关系
     */
    private CycleNextList<DataSourceCQL>   dataSourceCQLs;
    
    /**
     * 数据库连接的域。
     * 
     * 它可与 this.DataSourceCQL 同时存在值，但 this.domain 的优先级高。
     * 当"域"存在时，使用域的数据库连接池组。其它情况，使用默认的数据库连接池组。
     */
    private XCQLDomain                     domain;
    
    /** 数据库占位符CQL的信息 */
    private DBCQL                          content;
    
    /** 解释Xml文件，分析数据库结果集转化为Java实例对象 */
    private XCQLResult                     result;
    
    /** XCQL的触发器 */
    private XCQLTrigger                    trigger;
    
    /**
     * XCQL的触发器，在触发执行时，是否携带公共参数。
     * 当携带公共参数时，触发器的执行参数将被统一包装为Map类型，除了触发源原本的入参外，还将添加如下额外的参数
     *     1. XT_SourceID:    String类型，触发源的ID，即XCQL.getObjectID()
     *     2. XT_SourceXID:   String类型，触发源的XJavaID
     *     3. XT_StartTime:   Date类型，  触发源的执行开始时间
     *     4. XT_EndTime：    Date类型，  触发源的执行完成时间
     *     5. XT_IsError：    Integer类型，触发源的是否异常，0成功，1异常
     *     6. XT_ErrorInfo:   String类型，触发源的异常信息
     *     7. XT_ExecuteType: String类型，触发源的执行方式
     *     8. XT_IORowCount:  Integer类型，触发源的读写行数
     * 
     * 默认为：false
     * 
     * 注：triggerParams=true时，并且触发源的入参是List等复杂结构时，不再向触发器传递，仅传递上面和额外公共参数。
     *    同时，触发器的执行方法也有所改变，如executeUpdatesPrepared改成executes。，
     */
    private boolean                        triggerParams;
    
    /**
     * CQL类型。
     * 
     * N: 增、删、改、查的普通CQL语句  (默认值)
     * P: 存储过程
     * F: 函数
     * C: DML创建表，创建对象等
     */
    private String                         type;
    
    /**
     * 批量执行 Insert、Update、Delete 时，达到提交的提交点
     * 
     * 当>=1时，才有效，即分次提交
     * 当<=0时，在一个事务中执行所有的操作(默认状态)
     */
    private int                            batchCommit;
    
    /**
     * 是否允许或支持execute()方法中执行多条CQL语句，即$Executes_Split = ";/"分割符是否生效。
     * 默认情况下，通过XCQL模板自动判定$Executes_Split分割符是否生效的。
     * 
     * 但特殊情况下，允许外界通过本属性启用或关闭execute()方法中执行多条CQL语句的功能。
     * 如，CQL语句中就包含;/文本字符的情况，不是分割符是意思。
     */
    private boolean                        allowExecutesSplit;
    
    /** 唯一标示，主用于对比等操作 */
    private String                         uuid;
    
    /** 注释。可用于日志的输出等帮助性的信息 */
    private String                         comment;
    
    /** 可自行定制的XCQL异常处理机制 */
    private XCQLError                      error;
    
    /**
     * 执行CQL前的规则引擎。针对CQL参数、占位符的规则引擎
     * 
     * 优先级：触发的优先级高于“XCQL条件”
     * 
     * 注：无入参的不触发执行。
     */
    private XRule                          beforeRule;
    
    /**
     * 执行CQL后的规则引擎。针对CQL查询结果集的规则引擎。
     * 
     * 优先级：触发的优先级高于“XCQL应用级触发器”
     * 
     * 注1：只用于查询返回的XCQL。
     * 注2：getCount() 等简单数据结构的也不触发执行。
     */
    private XRule                          afterRule;
    
    
    
    public XCQL()
    {
        this.dataSourceCQLs     = new CycleNextList<DataSourceCQL>(1);
        this.domain             = null;
        this.content            = new DBCQL();
        this.result             = new XCQLResult();
        this.trigger            = null;
        this.triggerParams      = false;
        this.type               = $Type_NormalCQL;
        this.allowExecutesSplit = false;
        this.uuid               = StringHelp.getUUID();
        this.comment            = null;
        this.beforeRule         = null;
        this.afterRule          = null;
        this.error              = (XCQLError) xjavaGetObject($XCQLErrors);
    }
    
    
    
    /**
     * 获取对象池中的对象，在无XJava的环境中使用的兼容模式
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-05
     * @version     v1.0
     *
     * @param i_XID  对象XID
     * @return
     */
    private static Object xjavaGetObject(String i_XID)
    {
        try
        {
            // 用反射的方式执行 XJava.getObject()
            Method v_XJavaGetObjectMethod = Help.forName("org.hy.common.xml.XJava").getMethod("getObject" ,String.class);
            Object v_Object               = StaticReflect.invoke(v_XJavaGetObjectMethod ,i_XID);
            return v_Object;
        }
        catch (Exception exce)
        {
            $Logger.warn("未加载XJava组件" ,exce);
            return $CQLPool.get(i_XID);
        }
    }
    
    
    
    /**
     * 设置对象池中的对象，在无XJava的环境中使用的兼容模式
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-05
     * @version     v1.0
     *
     * @param i_XID      对象XID
     * @param i_XObject  对象
     * @return
     */
    private static void xjavaPutObject(String i_XID ,Object i_XObject)
    {
        try
        {
            // 用反射的方式执行 XJava.putObject()
            Method v_XJavaPutObjectMethod = Help.forName("org.hy.common.xml.XJava").getMethod("putObject" ,String.class ,Object.class);
            StaticReflect.invoke(v_XJavaPutObjectMethod ,i_XID ,i_XObject);
        }
        catch (Exception exce)
        {
            $Logger.warn("未加载XJava组件" ,exce);
            $CQLPool.put(i_XID ,i_XObject);
        }
    }
    
    
    
    /**
     * 删除对象池中的对象，在无XJava的环境中使用的兼容模式
     * 
     * @author      ZhengWei(HY)
     * @createDate  2025-11-24
     * @version     v1.0
     *
     * @param i_XID 对象XID
     * @return
     */
    private static void xjavaRemoveObject(String i_XID)
    {
        try
        {
            // 用反射的方式执行 XJava.remove()
            Method v_XJavaPutObjectMethod = Help.forName("org.hy.common.xml.XJava").getMethod("remove" ,String.class);
            StaticReflect.invoke(v_XJavaPutObjectMethod ,i_XID);
        }
        catch (Exception exce)
        {
            $Logger.warn("未加载XJava组件" ,exce);
            $CQLPool.remove(i_XID);
        }
    }
    
    
    
    /**
     * 重置统计数据
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-03-05
     * @version     v1.0
     *
     */
    @Override
    public void reset()
    {
        super.reset();
        
        if ( this.isTriggers() )
        {
            for (XCQLTriggerInfo v_XCQLTrigger : this.trigger.getXcqls())
            {
                v_XCQLTrigger.getXcql().reset();
            }
        }
    }
    
    
    
    /**
     * 数据请求时的统计
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-03-05
     * @version     v1.0
     *
     * @return
     */
    @Override
    protected Date request()
    {
        return super.request();
    }
    
    
    
    /**
     * 数据处理成功时的统计
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-03-05
     * @version     v1.0
     *
     * @param i_ExecuteTime  执行时间
     * @param i_TimeLen      用时时长（单位：毫秒）
     * @param i_SumCount     成功次数
     * @param i_IORowCount   读写行数
     */
    @Override
    protected void success(Date i_ExecuteTime ,double i_TimeLen ,int i_SumCount ,long i_IORowCount)
    {
        super.success(i_ExecuteTime ,i_TimeLen ,i_SumCount ,i_IORowCount);
    }
    
    
    
    /**
     * 检查数据库占位符CQL的对象是否为null。同时统计异常数据。
     * 
     * 此方法从各个数据库操作方法中提炼而来。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-12-19
     * @version     v1.0
     *
     */
    protected void checkContent()
    {
        if ( this.content == null )
        {
            NullPointerException v_Exce = new NullPointerException("Content is null of XCQL.");
            
            this.request();
            erroring("" ,v_Exce ,this);
            throw v_Exce;
        }
    }
    
    
    
    /**
     * 是否执行触发器
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-06
     * @version     v1.0
     *
     * @param i_IsError  主XCQL在执行时是否异常？
     * @return
     */
    protected boolean isTriggers(boolean i_IsError)
    {
        if ( this.isTriggers() )
        {
            if ( !i_IsError || this.trigger.isErrorMode() )
            {
                this.initTriggers();
                return true;
            }
        }
        
        return false;
    }
    
    
    
    /**
     * 是否有触发器
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-06
     * @version     v1.0
     *
     * @return
     */
    public boolean isTriggers()
    {
        return null != this.trigger && !Help.isNull(this.trigger.getXcqls());
    }
    
    
    
    /**
     * 对只有数据库连接组的触发器XCQL对象赋值
     * 
     *   这种情况一般是由：setCreateBackup(...) 方法创建的触发器
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-06
     * @version     v1.0
     *
     */
    private synchronized void initTriggers()
    {
        if ( !this.trigger.isInit() )
        {
            return;
        }
        
        for (XCQLTriggerInfo v_XCQLTrigger : this.trigger.getXcqls())
        {
            XCQL v_XCQL = v_XCQLTrigger.getXcql();
            
            if ( Help.isNull(v_XCQL.getContentDB().getCqlText()) )
            {
                v_XCQL.setXJavaID(    Help.NVL(this.getXJavaID()) + "_" + Date.getNowTime().getFullMilli_ID());
                v_XCQL.setContentDB(  this.getContentDB());
                v_XCQL.setResult(     this.getResult());
                v_XCQL.setType(       this.getType());
                v_XCQL.setDomain(     this.getDomain());
                v_XCQL.setBatchCommit(this.getBatchCommit());
                
                xjavaPutObject(v_XCQL.getXJavaID() ,v_XCQL);
            }
            
            if ( v_XCQL.getContentDB().getCqlText().indexOf(";/") >= 0 )
            {
                v_XCQLTrigger.setExecuteType(XCQLTrigger.$Execute);
            }
        }
        
        this.trigger.setInit(false);
    }
    
    
    
    /**
     * 通用(限定常用的数据库)分页查询。-- i_XCQL中的普通CQL，将通过模板变成一个分页CQL
     * 
     * 本方法并不真的执行查询，而是获取一个分页查询的XCQL对象。
     * 
     * 与游标分页查询相比，其性能是很高的。
     * 
     * CQL语句中的占位符 #StartIndex 下标从0开始
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-02-22
     * @version     v1.0
     *
     * @param i_XCQLID
     * @return
     */
    public static XCQL queryPaging(String i_XCQLID)
    {
        return XCQL.queryPaging((XCQL)xjavaGetObject(i_XCQLID) ,false);
    }
    
    
    
    /**
     * 通用(限定常用的数据库)分页查询。-- i_XCQL中的普通CQL，将通过模板变成一个分页CQL
     * 
     * 本方法并不真的执行查询，而是获取一个分页查询的XCQL对象。
     * 
     * 与游标分页查询相比，其性能是很高的。
     * 
     * CQL语句中的占位符 #StartIndex 下标从0开始
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-02-22
     * @version     v1.0
     *
     * @param i_XCQL
     * @return
     */
    public static XCQL queryPaging(XCQL i_XCQL)
    {
        return XCQL.queryPaging(i_XCQL ,false);
    }
    
    
    
    /**
     * 通用(限定常用的数据库)分页查询。
     * 
     * 本方法并不真的执行查询，而是获取一个分页查询的XCQL对象。
     * 
     * 与游标分页查询相比，其性能是很高的。
     * 
     * CQL语句中的占位符 #StartIndex 下标从0开始
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-02-22
     * @version     v1.0
     *
     * @param i_XCQL
     * @param i_IsClone  标示参数对象i_XCQL，是否会被改变。
     *                   1. 当为true时，用通用模板、具体i_XCQL生成一个全新的XCQL。
     *                   2. 当为false时，i_XCQL中的普通CQL，将通过模板变成一个分页CQL
     * @return
     */
    public static XCQL queryPaging(String i_XCQLID ,boolean i_IsClone)
    {
        return XCQL.queryPaging((XCQL)xjavaGetObject(i_XCQLID) ,i_IsClone);
    }
    
    
    
    /**
     * 通用(限定常用的数据库)分页查询。
     * 
     * 本方法并不真的执行查询，而是获取一个分页查询的XCQL对象。
     * 
     * 与游标分页查询相比，其性能是很高的。
     * 
     * CQL语句中的占位符 #StartIndex 下标从0开始
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-02-22
     * @version     v1.0
     *
     * @param i_XCQL
     * @param i_IsClone  标示参数对象i_XCQL，是否会被改变。
     *                   1. 当为true时，用通用模板、具体i_XCQL生成一个全新的XCQL。
     *                   2. 当为false时，i_XCQL中的普通CQL，将通过模板变成一个分页CQL
     * @return
     */
    public synchronized static XCQL queryPaging(XCQL i_XCQL ,boolean i_IsClone)
    {
        if ( null == i_XCQL )
        {
            return null;
        }
        
        String v_DBType = i_XCQL.getDataSourceCQL().getDbProductType();
        String v_PMKey  = v_DBType + "_" + i_XCQL.getObjectID();
        
        if ( $PagingMap.containsKey(v_PMKey) )
        {
            return $PagingMap.get(v_PMKey);
        }
        
        String v_PagingTemplate = null;
        if ( !Help.isNull(v_DBType) )
        {
            v_PagingTemplate = DBCQL.$Placeholder + "CQLPaging SKIP " + DBCQL.$Placeholder + "StartIndex LIMIT " + DBCQL.$Placeholder + "PagePerCount";
        }
        else
        {
            return null;
        }
        
        String v_PaginCQLText = StringHelp.replaceAll(v_PagingTemplate ,DBCQL.$Placeholder + "CQLPaging" ,i_XCQL.getContent().getCqlText());
        
        if ( i_IsClone )
        {
            // 用通用模板、具体i_XCQL生成一个全新的XCQL。
            // 优势：具体i_XCQL可零活使用，因为它本身没有变化，还可以用于非分页的查询。
            // 缺点：新XCQL与具体i_XCQL统计量不统一。
            XCQL v_NewXCQL = new XCQL();
            
            v_NewXCQL.setDataSourceCQL(i_XCQL.getDataSourceCQL());
            v_NewXCQL.setResult(       i_XCQL.getResult());
            v_NewXCQL.getContent().setCqlText(v_PaginCQLText);
            v_NewXCQL.setXJavaID("XPaging_" + v_PMKey);
            
            // 注意：这里是Key是i_XCQL，而不是v_NewXCQL的uuid
            $PagingMap.put(v_PMKey ,v_NewXCQL);
            xjavaPutObject(v_NewXCQL.getXJavaID() ,v_NewXCQL);
            return v_NewXCQL;
        }
        else
        {
            // 用通用模板替换具体i_XCQL中的内容。
            // 优势：统计功能统一。
            // 缺点：具体i_XCQL就变为专用于分页查询的CQL。
            i_XCQL.getContent().setCqlText(v_PaginCQLText);
            
            $PagingMap.put(v_PMKey ,i_XCQL);
            return i_XCQL;
        }
    }
    
    
    
    /**
     * 从缓存中删除之前生成的分页对象，好方便二次生成分页对象
     * 
     * @author      ZhengWei(HY)
     * @createDate  2025-11-24
     * @version     v1.0
     *
     * @param i_XJavaID
     */
    public synchronized static void removePaging(String i_XJavaID)
    {
        // 仅限于克隆生成的分页对象才能被删除
        // 对于非克隆生成的无法回退、无法二次生成分页对象
        if ( i_XJavaID.startsWith("XPaging_") )
        {
            xjavaRemoveObject(i_XJavaID);
            $PagingMap.remove(StringHelp.replaceFirst(i_XJavaID ,"XPaging_" ,""));
        }
    }
    
    
    
    /**
     * 占位符CQL的查询。 -- 无填充值的
     * 
     * @return
     */
    public Object query()
    {
        return XCQLOPQuery.queryXCQLData(this).getDatas();
    }
    
    
    
    /**
     * 占位符CQL的查询。 -- 无填充值的
     * 
     * @param i_Conn
     * @return
     */
    public Object query(Connection i_Conn)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Conn).getDatas();
    }
    
    
    
    /**
     * 占位符CQL的查询。游标的分页查询（可通用于所有数据库）。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-02-19
     * @version     v1.0
     * 
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public Object query(int i_StartRow ,int i_PagePerSize)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_StartRow ,i_PagePerSize).getDatas();
    }
    
    
    
    /**
     * 占位符CQL的查询。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return
     */
    public Object query(Map<String ,?> i_Values)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Values).getDatas();
    }
    
    
    
    /**
     * 占位符CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_Conn
     * @return
     */
    public Object query(Map<String ,?> i_Values ,Connection i_Conn)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Values ,i_Conn).getDatas();
    }
    
    
    
    /**
     * 占位符CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-02-19
     * @version     v1.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public Object query(Map<String ,?> i_Values ,int i_StartRow ,int i_PagePerSize)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Values ,i_StartRow ,i_PagePerSize).getDatas();
    }
    
    
    
    /**
     * 占位符CQL的查询。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return
     */
    public Object query(Object i_Obj)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Obj).getDatas();
    }
    
    
    
    /**
     * 占位符CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_Conn
     * @return
     */
    public Object query(Object i_Obj ,Connection i_Conn)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Obj ,i_Conn).getDatas();
    }
    
    
    
    /**
     * 占位符CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-02-19
     * @version     v1.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public Object query(Object i_Obj ,int i_StartRow ,int i_PagePerSize)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Obj ,i_StartRow ,i_PagePerSize).getDatas();
    }
    
    
    
    /**
     * 常规CQL的查询。
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_CQL              常规CQL语句
     * @return
     */
    public Object query(String i_CQL)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_CQL).getDatas();
    }
    
    
    
    /**
     * 常规CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_CQL              常规CQL语句
     * @return
     */
    public Object query(String i_CQL ,Connection i_Conn)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_CQL ,i_Conn).getDatas();
    }
    
    
    
    /**
     * 常规CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-02-19
     * @version     v1.0
     *
     * @param i_CQL              常规CQL语句
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public Object query(String i_CQL ,int i_StartRow ,int i_PagePerSize)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_CQL ,i_StartRow ,i_PagePerSize).getDatas();
    }
    
    
    
    /**
     * 占位符CQL的查询。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @return
     */
    public XCQLData queryXCQLData()
    {
        return XCQLOPQuery.queryXCQLData(this);
    }
    
    
    
    /**
     * 占位符CQL的查询。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Conn
     * @return
     */
    public XCQLData queryXCQLData(Connection i_Conn)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的查询。游标的分页查询（可通用于所有数据库）。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public XCQLData queryXCQLData(int i_StartRow ,int i_PagePerSize)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_StartRow ,i_PagePerSize);
    }
    
    
    
    /**
     * 占位符CQL的查询。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return
     */
    public XCQLData queryXCQLData(Map<String ,?> i_Values)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Values);
    }
    
    
    
    /**
     * 占位符CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_Conn
     * @return
     */
    public XCQLData queryXCQLData(Map<String ,?> i_Values ,Connection i_Conn)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Values ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public XCQLData queryXCQLData(Map<String ,?> i_Values ,int i_StartRow ,int i_PagePerSize)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Values ,i_StartRow ,i_PagePerSize);
    }
    
    
    
    /**
     * 占位符CQL的查询。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return
     */
    public XCQLData queryXCQLData(Object i_Obj)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Obj);
    }
    
    
    
    /**
     * 占位符CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_Conn
     * @return
     */
    public XCQLData queryXCQLData(Object i_Obj ,Connection i_Conn)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Obj ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public XCQLData queryXCQLData(Object i_Obj ,int i_StartRow ,int i_PagePerSize)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_Obj ,i_StartRow ,i_PagePerSize);
    }
    
    
    
    /**
     * 常规CQL的查询。
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_CQL              常规CQL语句
     * @return
     */
    public XCQLData queryXCQLData(String i_CQL)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_CQL);
    }
    
    
    
    /**
     * 常规CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_CQL              常规CQL语句
     * @return
     */
    public XCQLData queryXCQLData(String i_CQL ,Connection i_Conn)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_CQL ,i_Conn);
    }
    
    
    
    /**
     * 常规CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     *
     * @param i_CQL              常规CQL语句
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public XCQLData queryXCQLData(String i_CQL ,int i_StartRow ,int i_PagePerSize)
    {
        return XCQLOPQuery.queryXCQLData(this ,i_CQL ,i_StartRow ,i_PagePerSize);
    }
    

    
    /**
     * 查询返回第一行第一列上的数值。常用于查询返回一个字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-03-07
     * @version     v1.0
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return
     */
    public Object queryCQLValue(Map<String ,?> i_Values)
    {
        return XCQLOPQuery.queryCQLValue(this ,i_Values);
    }
    
    
    
    /**
     * 查询返回第一行第一列上的数值。常用于查询返回一个字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-03-07
     * @version     v1.0
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return
     */
    public Object queryCQLValue(Object i_Obj)
    {
        return XCQLOPQuery.queryCQLValue(this ,i_Obj);
    }
    
    
    
    /**
     * 查询返回第一行第一列上的数值。常用于查询返回一个字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-03-07
     * @version     v1.0
     * 
     * @return
     */
    public Object queryCQLValue()
    {
        return XCQLOPQuery.queryCQLValue(this);
    }
    
    
    
    /**
     * 查询返回第一行第一列上的数值。常用于查询返回一个字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-03-07
     * @version     v1.0
     * 
     * @param i_CQL  查询CQL
     * @return
     */
    public Object queryCQLValue(String i_CQL)
    {
        return XCQLOPQuery.queryCQLValue(this ,i_CQL);
    }
    
    
    
    /**
     * 统计记录数据：占位符CQL的查询。
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return
     */
    public long queryCQLCount(Map<String ,?> i_Values)
    {
        return XCQLOPQuery.queryCQLCount(this ,i_Values);
    }
    
    
    
    /**
     * 统计记录数据：占位符CQL的查询。
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return
     */
    public long queryCQLCount(Object i_Obj)
    {
        return XCQLOPQuery.queryCQLCount(this ,i_Obj);
    }
    
    
    
    /**
     * 查询记录总数
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * @return
     */
    public long queryCQLCount()
    {
        return XCQLOPQuery.queryCQLCount(this);
    }
    
    
    
    /**
     * 查询记录总数
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * @param i_CQL
     * @return
     */
    public long queryCQLCount(String i_CQL)
    {
        return XCQLOPQuery.queryCQLCount(this ,i_CQL);
    }
    
    
    
    /**
     * 统计记录数据：占位符CQL的查询。
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return
     */
    public long getCQLCount(Map<String ,?> i_Values)
    {
        return XCQLOPQuery.queryCQLCount(this ,i_Values);
    }
    
    
    
    /**
     * 统计记录数据：占位符CQL的查询。
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return
     */
    public long getCQLCount(Object i_Obj)
    {
        return XCQLOPQuery.queryCQLCount(this ,i_Obj);
    }
    
    
    
    /**
     * 查询记录总数
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * @param i_CQL
     * @return
     */
    public long getCQLCount(String i_CQL)
    {
        return XCQLOPQuery.queryCQLCount(this ,i_CQL);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句的执行。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_XCQL
     * @return        返回语句影响的记录数及自增长ID。
     */
    public XCQLData executeInsert()
    {
        return XCQLOPInsert.executeInsert(this);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句的执行。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert语法的写入操作。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public XCQLData executeInsert(final Map<String ,?> i_Values)
    {
        return XCQLOPInsert.executeInsert(this ,i_Values);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert语法的写入操作。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public XCQLData executeInsert(final Object i_Obj)
    {
        return XCQLOPInsert.executeInsert(this ,i_Obj);
    }
    
    
    
    /**
     * 常规Insert语句的执行。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public XCQLData executeInsert(final String i_CQL)
    {
        return XCQLOPInsert.executeInsert(this ,i_CQL);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句的执行。 -- 无填充值的（内部不再关闭数据库连接）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Conn             数据库连接
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public XCQLData executeInsert(final Connection i_Conn)
    {
        return XCQLOPInsert.executeInsert(this ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_Conn             数据库连接
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public XCQLData executeInsert(final Map<String ,?> i_Values ,final Connection i_Conn)
    {
        return XCQLOPInsert.executeInsert(this ,i_Values ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_Conn             数据库连接
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public XCQLData executeInsert(final Object i_Obj ,final Connection i_Conn)
    {
        return XCQLOPInsert.executeInsert(this ,i_Obj ,i_Conn);
    }
    
    
    
    /**
     * 常规Insert语句的执行。（内部不再关闭数据库连接）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @param i_Conn             数据库连接
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public XCQLData executeInsert(final String i_CQL ,final Connection i_Conn)
    {
        return XCQLOPInsert.executeInsert(this ,i_CQL ,i_Conn);
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句与Update语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_ObjList          占位符CQL的填充对象的集合。
     *                           1. 集合元素可以是Object
     *                           2. 集合元素可以是Map<String ,?>
     *                           3. 更可以是上面两者的混合元素组成的集合
     * @return                   返回语句影响的记录数。
     */
    public XCQLData executeInserts(final List<?> i_ObjList)
    {
        return XCQLOPInsert.executeInserts(this ,i_ObjList);
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句的执行。
     * 
     *   注意：不支持Delete语句
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_ObjList          占位符CQL的填充对象的集合。
     *                           1. 集合元素可以是Object
     *                           2. 集合元素可以是Map<String ,?>
     *                           3. 更可以是上面两者的混合元素组成的集合
     * @param i_Conn             数据库连接。
     *                           1. 当为空时，内部自动获取一个新的数据库连接。
     *                           2. 当有值时，内部将不关闭数据库连接，而是交给外部调用者来关闭。
     *                           3. 当有值时，内部也不执行"提交"操作（但分批提交this.batchCommit大于0时除外），而是交给外部调用者来执行"提交"。
     *                           4. 当有值时，出现异常时，内部也不执行"回滚"操作，而是交给外部调用者来执行"回滚"。
     * @return                   返回语句影响的记录数。
     */
    public XCQLData executeInserts(final List<?> i_ObjList ,final Connection i_Conn)
    {
        return XCQLOPInsert.executeInserts(this ,i_ObjList ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句与Update语句的执行。 -- 无填充值的
     * 
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdate()
    {
        return XCQLOPUpdate.executeUpdate(this);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句与Update语句的执行。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert、Update语法的写入操作。
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdate(Map<String ,?> i_Values)
    {
        return XCQLOPUpdate.executeUpdate(this ,i_Values);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句与Update语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert、Update语法的写入操作。
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdate(Object i_Obj)
    {
        return XCQLOPUpdate.executeUpdate(this ,i_Obj);
    }
    
    
    
    /**
     * 常规Insert语句与Update语句的执行。
     * 
     * @param i_CQL              常规CQL语句
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdate(String i_CQL)
    {
        return XCQLOPUpdate.executeUpdate(this ,i_CQL);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句与Update语句的执行。 -- 无填充值的（内部不再关闭数据库连接）
     * 
     * @param i_Conn             数据库连接
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdate(Connection i_Conn)
    {
        return XCQLOPUpdate.executeUpdate(this ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句与Update语句的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_Conn             数据库连接
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdate(Map<String ,?> i_Values ,Connection i_Conn)
    {
        return XCQLOPUpdate.executeUpdate(this ,i_Values ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的Insert语句与Update语句的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_Conn             数据库连接
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdate(Object i_Obj ,Connection i_Conn)
    {
        return XCQLOPUpdate.executeUpdate(this ,i_Obj ,i_Conn);
    }
    
    
    
    /**
     * 常规Insert语句与Update语句的执行。（内部不再关闭数据库连接）
     * 
     * @param i_CQL              常规CQL语句
     * @param i_Conn             数据库连接
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdate(String i_CQL ,Connection i_Conn)
    {
        return XCQLOPUpdate.executeUpdate(this ,i_CQL ,i_Conn);
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句与Update语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @param i_ObjList          占位符CQL的填充对象的集合。
     *                           1. 集合元素可以是Object
     *                           2. 集合元素可以是Map<String ,?>
     *                           3. 更可以是上面两者的混合元素组成的集合
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdates(List<?> i_ObjList)
    {
        return XCQLOPUpdate.executeUpdates(this ,i_ObjList);
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句与Update语句的执行。
     * 
     *   注意：不支持Delete语句
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @param i_ObjList          占位符CQL的填充对象的集合。
     *                           1. 集合元素可以是Object
     *                           2. 集合元素可以是Map<String ,?>
     *                           3. 更可以是上面两者的混合元素组成的集合
     * @param i_Conn             数据库连接。
     *                           1. 当为空时，内部自动获取一个新的数据库连接。
     *                           2. 当有值时，内部将不关闭数据库连接，而是交给外部调用者来关闭。
     *                           3. 当有值时，内部也不执行"提交"操作（但分批提交this.batchCommit大于0时除外），而是交给外部调用者来执行"提交"。
     *                           4. 当有值时，出现异常时，内部也不执行"回滚"操作，而是交给外部调用者来执行"回滚"。
     * @return  返回语句影响的记录数。
     *            当 getID=false 时，返回值表示：影响的记录行数
     *            当 getID=true  时，返回值表示：写入首条记录的自增长ID的值。影响0行时，返回0
     */
    public int executeUpdates(List<?> i_ObjList ,Connection i_Conn)
    {
        return XCQLOPUpdate.executeUpdates(this ,i_ObjList ,i_Conn);
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句与Update语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注: 1. 支持多种不同CQL语句的执行
     *     2. 支持不同类型的多个不同数据库的操作
     *     3. 如果要有顺序的执行，请java.util.LinkedHashMap
     * 
     * 重点注意：2014-12-04
     *         建议入参使用 TablePartition。为什么呢？
     *         原因是，Hashtable.put() 同样的key多次，只保存一份value。
     *         而 TablePartition.putRows() 会将同样key的多份不同的value整合在一起。
     *         特别适应于同一份Insert语句的CQL，执行多批数据的插入的情况
     * 
     * @param i_XCQLs            XCQL及占位符CQL的填充对象的集合。
     *                           1. List<?>集合元素可以是Object
     *                           2. List<?>集合元素可以是Map<String ,?>
     *                           3. List<?>更可以是上面两者的混合元素组成的集合
     * @return                   返回语句影响的记录数。
     */
    @SuppressWarnings({"rawtypes" ,"unchecked"})
    public static int executeUpdates(PartitionMap<XCQL ,?> i_XCQLs)
    {
        return XCQLOPUpdate.executeUpdates((Map)i_XCQLs ,0);
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句与Update语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注: 1. 支持多种不同CQL语句的执行
     *     2. 支持不同类型的多个不同数据库的操作
     *     3. 如果要有顺序的执行，请java.util.LinkedHashMap
     * 
     * 重点注意：2014-12-04
     *         建议入参使用 TablePartition。为什么呢？
     *         原因是，Hashtable.put() 同样的key多次，只保存一份value。
     *         而 TablePartition.putRows() 会将同样key的多份不同的value整合在一起。
     *         特别适应于同一份Insert语句的CQL，执行多批数据的插入的情况
     * 
     * @param i_XCQLs            XCQL及占位符CQL的填充对象的集合。
     *                           1. List<?>集合元素可以是Object
     *                           2. List<?>集合元素可以是Map<String ,?>
     *                           3. List<?>更可以是上面两者的混合元素组成的集合
     * @return                   返回语句影响的记录数。
     */
    public static int executeUpdates(Map<XCQL ,List<?>> i_XCQLs)
    {
        return XCQLOPUpdate.executeUpdates(i_XCQLs ,0);
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句与Update语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注: 1. 支持多种不同CQL语句的执行
     *     2. 支持不同类型的多个不同数据库的操作
     *     3. 如果要有顺序的执行，请java.util.LinkedHashMap
     *
     * 重点注意：2014-12-04
     *         建议入参使用 TablePartition<XCQL ,?>，（注意不是 TablePartition<XCQL ,List<?>>）
     *         为什么呢？
     *         原因是，Hashtable.put() 同样的key多次，只保存一份value。
     *         而 TablePartition.putRows() 会将同样key的多份不同的value整合在一起。
     *         特别适应于同一份Insert语句的CQL，执行多批数据的插入的情况
     * 
     * @param i_XCQLs            XCQL及占位符CQL的填充对象的集合。
     *                           1. List<?>集合元素可以是Object
     *                           2. List<?>集合元素可以是Map<String ,?>
     *                           3. List<?>更可以是上面两者的混合元素组成的集合
     * @param i_BatchCommit      批量执行 Insert、Update、Delete 时，达到提交的提交点
     * @return                   返回语句影响的记录数。
     */
    public static <R> int executeUpdates(Map<XCQL ,List<?>> i_XCQLs ,int i_BatchCommit)
    {
        return XCQLOPUpdate.executeUpdates(i_XCQLs ,i_BatchCommit);
    }
    
    
    
    /**
     * 占位符CQL的执行。-- 无填充值的
     * 
     * @return                   是否执行成功。
     */
    public boolean execute()
    {
        return XCQLOPDDL.execute(this);
    }
    
    
    
    /**
     * 占位符CQL的执行。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert、Update语法的写入操作。
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return                   是否执行成功。
     */
    public boolean execute(Map<String ,?> i_Values)
    {
        return XCQLOPDDL.execute(this ,i_Values);
    }
    
    
    
    /**
     * 占位符CQL的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert、Update语法的写入操作。
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return                   是否执行成功。
     */
    public boolean execute(Object i_Obj)
    {
        return XCQLOPDDL.execute(this ,i_Obj);
    }
    
    
    
    /**
     * 常规CQL的执行。
     * 
     * @param i_CQL              常规CQL语句
     * @return                   是否执行成功。
     */
    public boolean execute(String i_CQL)
    {
        return XCQLOPDDL.execute(this ,i_CQL);
    }
    
    
    
    /**
     * 占位符CQL的执行。-- 无填充值的（内部不再关闭数据库连接）
     * 
     * @param i_Conn             数据库连接
     * @return                   是否执行成功。
     */
    public boolean execute(Connection i_Conn)
    {
        return XCQLOPDDL.execute(this ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_Conn             数据库连接
     * @return                   是否执行成功。
     */
    public boolean execute(Map<String ,?> i_Values ,Connection i_Conn)
    {
        return XCQLOPDDL.execute(this ,i_Values ,i_Conn);
    }
    
    
    
    /**
     * 占位符CQL的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_Conn             数据库连接
     * @return                   是否执行成功。
     */
    public boolean execute(Object i_Obj ,Connection i_Conn)
    {
        return XCQLOPDDL.execute(this ,i_Obj ,i_Conn);
    }
    
    
    
    /**
     * 常规CQL的执行。（内部不再关闭数据库连接）
     * 
     * @param i_CQL              常规CQL语句
     * @param i_Conn             数据库连接
     * @return                   是否执行成功。
     */
    public boolean execute(String i_CQL ,Connection i_Conn)
    {
        return XCQLOPDDL.execute(this ,i_CQL ,i_Conn);
    }
    
    
    
    /**
     * 关闭外部所有与数据有关的连接
     * 
     * @param i_Resultset
     * @param i_Transaction
     * @param i_Conn
     */
    public void closeDB(Result i_Resultset ,Connection i_Conn)
    {
        try
        {
            if ( i_Conn != null )
            {
                i_Conn.close();
            }
        }
        catch (Throwable exce)
        {
            $Logger.error(exce);
        }
        finally
        {
            i_Conn = null;
        }
    }
    
    
    
    public XCQLResult getResult()
    {
        return result;
    }
    
    
    
    public void setResult(XCQLResult i_Result)
    {
        this.result = i_Result;
        
        if ( this.beforeRule != null )
        {
            this.setBeforeRule(this.beforeRule);
        }
        
        if ( this.afterRule != null )
        {
            this.setAfterRule(this.afterRule);
        }
    }
    
    
    
    public DBCQL getContent()
    {
        return this.content;
    }
    
    
    
    public void setContent(String i_CQLText)
    {
        this.content.setCqlText(i_CQLText);
        this.isAllowExecutesSplit(i_CQLText);
    }
    
    
    
    /**
     * 占位符X有条件的取值。占位符在满足条件时取值A，否则取值B。
     * 取值A、B，可以是占位符X、NULL值，另一个占位符Y或常量字符。
     * 
     * 类似于Mybatis IF条件功能
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-08-10
     * @version     v1.0
     *              v2.0  2019-01-20  添加：升级来条件组，用于实现Java编程语言中的 if .. else if ... else ... 的多条件复杂判定
     *
     * @param i_ConditionGroup
     */
    public void setCondition(DBConditions i_ConditionGroup)
    {
        this.content.addCondition(i_ConditionGroup);
    }
    
    
    
    public DBCQL getContentDB()
    {
        return this.content;
    }
    
    
    
    public void setContentDB(DBCQL i_DBCQL)
    {
        this.content = i_DBCQL;
        
        if ( this.content != null )
        {
            this.isAllowExecutesSplit(this.content.getCqlText());
        }
    }
    
    
    
    /**
     * 默认情况下，通过XCQL模板自动判定$Executes_Split分割符是否生效的。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-06-24
     * @version     v1.0
     *
     * @param i_CQLText
     */
    private void isAllowExecutesSplit(String i_CQLText)
    {
        if ( !Help.isNull(i_CQLText) )
        {
            if ( i_CQLText.split($Executes_Split).length >= 2 )
            {
                this.allowExecutesSplit = true;
            }
            else
            {
                this.allowExecutesSplit = false;
            }
        }
        else
        {
            this.allowExecutesSplit = false;
        }
    }
    
    
    
    /**
     * 是否允许或支持execute()方法中执行多条CQL语句，即$Executes_Split = ";/"分割符是否生效。
     * 默认情况下，通过XCQL模板自动判定$Executes_Split分割符是否生效的。
     * 
     * 但特殊情况下，允许外界通过本属性启用或关闭execute()方法中执行多条CQL语句的功能。
     * 如，CQL语句中就包含;/文本字符的情况，不是分割符是意思。
     */
    public boolean isAllowExecutesSplit()
    {
        return allowExecutesSplit;
    }

    

    /**
     * 是否允许或支持execute()方法中执行多条CQL语句，即$Executes_Split = ";/"分割符是否生效。
     * 默认情况下，通过XCQL模板自动判定$Executes_Split分割符是否生效的。
     * 
     * 但特殊情况下，允许外界通过本属性启用或关闭execute()方法中执行多条CQL语句的功能。
     * 如，CQL语句中就包含;/文本字符的情况，不是分割符是意思。
     */
    public void setAllowExecutesSplit(boolean allowExecutesSplit)
    {
        this.allowExecutesSplit = allowExecutesSplit;
    }
    
    

    /**
     * 多个数据库连接批量提交
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-01-21
     * @version     v1.0
     *
     * @param i_Strategy  策略类型。
     *                       策略1：当出现异常时，其后的连接均继续：提交。
     *                       策略2：当出现异常时，其后的连接均执行：回滚。
     * @param i_Conns     数据库连接的集合
     * @return
     */
    public static boolean commits(int i_Strategy ,List<Connection> i_Conns)
    {
        boolean v_IsOK = true;
        
        if ( Help.isNull(i_Conns) )
        {
            return v_IsOK;
        }

        // 策略1：当出现异常时，其后的连接均继续：提交。
        if ( i_Strategy == 1 )
        {
            for (Connection v_Conn : i_Conns)
            {
                try
                {
                    v_Conn.beginTransaction().commit();
                }
                catch (Exception exce)
                {
                    v_IsOK = false;
                    $Logger.error(exce);
                    exce.printStackTrace();
                }
            }
        }
        // 策略2：当出现异常时，其后的连接均执行：回滚。
        else if ( i_Strategy == 2 )
        {
            for (Connection v_Conn : i_Conns)
            {
                try
                {
                    if ( v_IsOK )
                    {
                        v_Conn.beginTransaction().commit();
                    }
                    else
                    {
                        v_Conn.beginTransaction().rollback();
                    }
                }
                catch (Exception exce)
                {
                    v_IsOK = false;
                    $Logger.error(exce);
                    exce.printStackTrace();
                }
            }
        }
        
        return v_IsOK;
    }
    
    
    
    /**
     * 多个数据库连接批量回滚
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-01-21
     * @version     v1.0
     *
     * @param i_Conns     数据库连接的集合
     * @return
     */
    public static boolean rollbacks(List<Connection> i_Conns)
    {
        boolean v_IsOK = true;

        if ( !Help.isNull(i_Conns) )
        {
            for (Connection v_Conn : i_Conns)
            {
                try
                {
                    v_Conn.beginTransaction().rollback();
                }
                catch (Exception exce)
                {
                    // 异常用不抛出
                    v_IsOK = false;
                }
            }
        }
        
        return v_IsOK;
    }
    
    
    
    /**
     * 多个数据库连接批量关闭
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-01-21
     * @version     v1.0
     *
     * @param i_Conns     数据库连接的集合
     * @return
     */
    public static boolean closeDB(List<Connection> i_Conns)
    {
        boolean v_IsOK = true;

        if ( !Help.isNull(i_Conns) )
        {
            for (Connection v_Conn : i_Conns)
            {
                try
                {
                    v_Conn.close();
                }
                catch (Exception exce)
                {
                    // 异常用不抛出
                    v_IsOK = false;
                }
            }
        }
        
        return v_IsOK;
    }
    
    
    
    /**
     * 获取数据库连接。
     * 
     * @return
     */
    public Connection getConnection(DataSourceCQL i_DataSourceCQL)
    {
        return i_DataSourceCQL.getConnection();
    }

    
    
    /**
     * 获取：数据库连接池组
     * 
     * 当"域"存在时，使用域的数据库连接池组。其它情况，使用默认的数据库连接池组。
     */
    public DataSourceCQL getDataSourceCQL()
    {
        if ( this.domain != null )
        {
            try
            {
                DataSourceCQL v_DomainDBG = this.domain.getDataSourceCQL();
                
                if ( v_DomainDBG != null )
                {
                    return v_DomainDBG;
                }
            }
            catch (Exception exce)
            {
                $Logger.error(exce);
                exce.printStackTrace();
            }
        }
        
        return this.dataSourceCQLs.next();
    }

    
    
    /**
     * 设置：将数据库连接池组将添加到的负载数据库集合中
     * 
     * 当添加多个“数据库连接池”时，可实现多个平行、平等的数据库的负载均衡（简单级的）。
     * 目前建议只用在查询CQL上，当多个相同数据的数据库（如主备数据库），
     * 在高并发的情况下，提高整体查询速度，查询锁、查询阻塞等问题均能得到一定的解决。
     * 在高并发的情况下，突破数据库可分配的连接数量，会话数量将翻数倍（与数据库个数有正相关的关系
     * 
     * @param i_DataSourceCQL
     */
    public void setDataSourceCQL(DataSourceCQL i_DataSourceCQL)
    {
        if ( i_DataSourceCQL == null ) return;
        this.dataSourceCQLs.add(i_DataSourceCQL);
    }
    
    
    
    /**
     * 获取：数据库连接的域
     * 
     * 它可与 this.DataSourceCQL 同时存在值，但 this.domain 的优先级高。
     * 当"域"存在时，使用域的数据库连接池组。其它情况，使用默认的数据库连接池组。
     */
    public XCQLDomain getDomain()
    {
        return domain;
    }

    
    
    /**
     * 设置：数据库连接的域
     * 
     * 它可与 this.DataSourceCQL 同时存在值，但 this.domain 的优先级高。
     * 当"域"存在时，使用域的数据库连接池组。其它情况，使用默认的数据库连接池组。
     * 
     * @param domain
     */
    public void setDomain(XCQLDomain domain)
    {
        this.domain = domain;
    }
    
    
    
    /**
     * 设置XJava池中对象的ID标识。此方法不用用户调用设置值，是自动的。
     * 
     * @param i_XJavaID
     */
    @Override
    public void setXJavaID(String i_XJavaID)
    {
        this.xjavaID = i_XJavaID;
    }
    
    
    
    /**
     * 获取XJava池中对象的ID标识。
     * 
     * @return
     */
    @Override
    public String getXJavaID()
    {
        return this.xjavaID;
    }
    


    /**
     * 获取：XCQL的触发器
     */
    public XCQLTrigger getTrigger()
    {
        return trigger;
    }

    
    
    /**
     * 设置：XCQL的触发器
     * 
     * @param trigger
     */
    public void setTrigger(XCQLTrigger trigger)
    {
        this.trigger = trigger;
    }
    
    
    
    /**
     * 获取：XCQL的触发器，在触发执行时，是否携带公共参数。
     * 当携带公共参数时，触发器的执行参数将被统一包装为Map类型，除了触发源原本的入参外，还将添加如下额外的参数
     *     1. XT_SourceID:    String类型，触发源的ID，即XCQL.getObjectID()
     *     2. XT_SourceXID:   String类型，触发源的XJavaID
     *     3. XT_StartTime:   Date类型，  触发源的执行开始时间
     *     4. XT_EndTime：    Date类型，  触发源的执行完成时间
     *     5. XT_IsError：    Integer类型，触发源的是否异常，0成功，1异常
     *     6. XT_ErrorInfo:   String类型，触发源的异常信息
     *     7. XT_ExecuteType: String类型，触发源的执行方式
     *     8. XT_IORowCount:  Integer类型，触发源的读写行数
     * 
     * 默认为：false，
     */
    public boolean isTriggerParams()
    {
        return triggerParams;
    }


    
    /**
     * 设置：XCQL的触发器，在触发执行时，是否携带公共参数。
     * 当携带公共参数时，触发器的执行参数将被统一包装为Map类型，除了触发源原本的入参外，还将添加如下额外的参数
     *     1. XT_SourceID:    String类型，触发源的ID，即XCQL.getObjectID()
     *     2. XT_SourceXID:   String类型，触发源的XJavaID
     *     3. XT_StartTime:   Date类型，  触发源的执行开始时间
     *     4. XT_EndTime：    Date类型，  触发源的执行完成时间
     *     5. XT_IsError：    Integer类型，触发源的是否异常，0成功，1异常
     *     6. XT_ErrorInfo:   String类型，触发源的异常信息
     *     7. XT_ExecuteType: String类型，触发源的执行方式
     *     8. XT_IORowCount:  Integer类型，触发源的读写行数
     * 
     * 默认为：false
     * 
     * @param i_TriggerParams
     */
    public void setTriggerParams(boolean i_TriggerParams)
    {
        this.triggerParams = i_TriggerParams;
    }
    
    
    
    /**
     * 触发源执行前，生成触发器额外附加参数
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-10-17
     * @version     v1.0
     *
     * @param i_ExecuteType  触发源的执行方式
     * @return
     */
    protected Map<String ,Object> executeBeforeForTrigger(String i_ExecuteType ,Object i_XSQLParam)
    {
        Map<String ,Object> v_Params = null;
        
        if ( this.triggerParams )
        {
            if ( i_XSQLParam != null )
            {
                try
                {
                    v_Params = Help.toMap(i_XSQLParam ,null ,true ,false ,true);
                }
                catch (Exception e)
                {
                    // 异常时，也继续
                    $Logger.error(e);
                    v_Params = new HashMap<String ,Object>();
                }
            }
            else
            {
                v_Params = new HashMap<String ,Object>();
            }
            
            v_Params.put($Trigger_SourceID    ,this.getObjectID());
            v_Params.put($Trigger_SourceXID   ,this.getXJavaID());
            v_Params.put($Trigger_StartTime   ,new Date());
            v_Params.put($Trigger_ExecuteType ,i_ExecuteType);
        }
        
        return v_Params;
    }
    
    
    
    /**
     * 触发源执行前，生成触发器额外附加参数
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-10-17
     * @version     v1.0
     *
     * @param i_ExecuteType  触发源的执行方式
     * @param i_XSQLParam    触发源的执行参数（禁止修改、添加、删除任务元素）
     * @return
     */
    protected Map<String ,Object> executeBeforeForTrigger(String i_ExecuteType ,final Map<String ,?> i_XSQLParam)
    {
        Map<String ,Object> v_Params = null;
        
        if ( this.triggerParams )
        {
            v_Params = new HashMap<String ,Object>();
            
            if ( !Help.isNull(i_XSQLParam) )
            {
                for (Map.Entry<String, ?> v_Item : i_XSQLParam.entrySet())
                {
                    v_Params.put(v_Item.getKey() ,v_Item.getValue());
                }
            }
            
            v_Params.put($Trigger_SourceID    ,this.getObjectID());
            v_Params.put($Trigger_SourceXID   ,this.getXJavaID());
            v_Params.put($Trigger_StartTime   ,new Date());
            v_Params.put($Trigger_ExecuteType ,i_ExecuteType);
        }
        
        return v_Params;
    }
    
    
    
    /**
     * 触发源执行后，生成触发器额外附加参数
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-10-17
     * @version     v1.0
     *
     * @param io_TriggerParams  触发器额外附加参数
     * @param i_IORowCount      读写行数
     * @param i_ErrorInfo       异常信息。为空和空字符串均表示无异常
     * @return
     */
    protected Map<String ,Object> executeAfterForTrigger(Map<String ,Object> io_TriggerParams ,long i_IORowCount ,String i_ErrorInfo)
    {
        Map<String ,Object> v_Params = io_TriggerParams;
        
        v_Params.put($Trigger_EndTime    ,new Date());
        v_Params.put($Trigger_IORowCount ,i_IORowCount);
        v_Params.put($Trigger_IsError    ,Help.isNull(i_ErrorInfo) ? 0 : 1);
        v_Params.put($Trigger_ErrorInfo  ,Help.NVL(i_ErrorInfo));
        
        return v_Params;
    }
    
    
    
    /**
     * CQL类型。
     * 
     * N: 增、删、改、查的普通CQL语句  (默认值)
     * P: 存储过程
     * F: 函数
     * C: DML创建表，创建对象等
     */
    public String getType()
    {
        return Help.NVL(this.type ,$Type_NormalCQL);
    }


    
    /**
     * CQL类型。
     * 
     * N: 增、删、改、查的普通CQL语句  (默认值)
     * P: 存储过程
     * F: 函数
     * C: DML创建表，创建对象等
     */
    public void setType(String i_Type)
    {
        if ( Help.isNull(i_Type) )
        {
            throw new NullPointerException("Type is null.");
        }
        
        if ( !$Type_NormalCQL.equals(i_Type)
          && !$Type_Create   .equals(i_Type) )
        {
            throw new IllegalArgumentException("Type is not 'N' or 'C' of XCQL.");
        }
        
        this.type = i_Type;
    }


    
    /**
     * 批量执行 Insert、Update、Delete 时，达到提交的提交点
     * 
     * 当>=1时，才有效，即分次提交
     * 当<=0时，在一个事务中执行所有的操作(默认状态)
     *
     * @return
     */
    public int getBatchCommit()
    {
        return batchCommit;
    }


    
    /**
     * 批量执行 Insert、Update、Delete 时，达到提交的提交点
     * 
     * 当>=1时，才有效，即分次提交
     * 当<=0时，在一个事务中执行所有的操作(默认状态)
     *
     * @param i_BatchCommit
     */
    public void setBatchCommit(int i_BatchCommit)
    {
        this.batchCommit = i_BatchCommit;
    }
    
    
    
    /**
     * 获取：注释。可用于日志的输出等帮助性的信息
     */
    @Override
    public String getComment()
    {
        return comment;
    }
    

    
    /**
     * 设置：注释。可用于日志的输出等帮助性的信息
     * 
     * @param comment
     */
    @Override
    public void setComment(String comment)
    {
        this.comment = comment;
    }
    


    public String getObjectID()
    {
        return this.uuid;
    }
    
    
    
    /**
     * 允许外界重新定义对象ID
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-10-17
     * @version     v1.0
     *
     * @param i_ID
     */
    public void setObjectID(String i_ID)
    {
        this.uuid = i_ID;
    }
    
    
    
    /**
     * 执行CQL异常时的统一处理方法
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-04
     * @version     v1.0
     *
     * @param i_CQL
     * @param i_Exce
     * @param i_XCQL
     */
    protected static void erroring(String i_CQL ,Exception i_Exce ,XCQL i_XCQL)
    {
        XCQLLog v_XCQLLog = new XCQLLog(i_CQL ,i_Exce ,i_XCQL == null ? "" : i_XCQL.getObjectID());

        $CQLBuswayTP   .putRow(i_XCQL.getObjectID() ,v_XCQLLog);
        $CQLBusway     .put(v_XCQLLog);
        $CQLBuswayError.put(v_XCQLLog);
        
        String v_XJavaID = "";
        
        if ( i_XCQL != null )
        {
            v_XJavaID = Help.NVL(i_XCQL.getXJavaID());
            
            if ( i_XCQL.getDataSourceCQL() != null )
            {
                i_XCQL.getDataSourceCQL().setException(true);
            }
        }
        
        $Logger.error("\n-- Error time:    " + Date.getNowTime().getFull()
                    + "\n-- Error XCQL ID: " + v_XJavaID
                    + "\n-- Error CQL:     " + i_CQL ,i_Exce);
        
        i_Exce.printStackTrace();
    }
    
    
    
    /**
     * 获取：可自行定制的XCQL异常处理机制
     */
    public XCQLError getError()
    {
        return error;
    }



    /**
     * 设置：可自行定制的XCQL异常处理机制
     * 
     * @param error
     */
    public void setError(XCQLError error)
    {
        this.error = error;
    }
    
    
    
    /**
     * 执行之后的日志。（在CQL语法成功执行之后，在this.result.getDatas(...)方法之前执行）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-05-15
     * @version     v1.0
     *
     * @param i_CQL
     */
    protected void log(String i_CQL)
    {
        XCQLLog v_XCQLLog = new XCQLLog(i_CQL ,null ,this.getObjectID());
        
        $CQLBuswayTP.putRow(this.getObjectID() ,v_XCQLLog);
        $CQLBusway  .put(v_XCQLLog);
        
        StringBuilder v_Buffer = new StringBuilder();
        if ( !Help.isNull(this.xjavaID) )
        {
            v_Buffer.append(this.xjavaID);
            
            if ( !Help.isNull(this.comment) )
            {
                v_Buffer.append(" : ").append(this.comment).append("\n");
            }
            else
            {
                v_Buffer.append("\n");
            }
        }
        else
        {
            if ( !Help.isNull(this.comment) )
            {
                v_Buffer.append(this.comment).append("\n");
            }
        }
        
        v_Buffer.append(i_CQL);
        $Logger.debug(v_Buffer.toString());
    }
    
    
    
    /**
     * 获取：执行CQL前的规则引擎。针对CQL参数、占位符的规则引擎
     * 
     * 优先级：触发的优先级高于“XCQL条件”
     * 
     * 注：无入参的不触发执行。
     */
    public XRule getBeforeRule()
    {
        return beforeRule;
    }


    
    /**
     * 获取：执行CQL后的规则引擎。针对CQL查询结果集的规则引擎。
     * 
     * 优先级：触发的优先级高于“XCQL应用级触发器”
     * 
     * 注1：只用于查询返回的XCQL。
     * 注2：getCount() 等简单数据结构的也不触发执行。
     */
    public XRule getAfterRule()
    {
        return afterRule;
    }


    
    /**
     * 设置：执行CQL前的规则引擎。针对CQL参数、占位符的规则引擎
     * 
     * 优先级：触发的优先级高于“XCQL条件”
     * 
     * 注：无入参的不触发执行。
     * 
     * @param i_BeforeRule
     */
    public void setBeforeRule(XRule i_BeforeRule)
    {
        this.beforeRule = this.addPackageImports(i_BeforeRule);
    }


    
    /**
     * 设置：执行CQL后的规则引擎。针对CQL查询结果集的规则引擎。
     * 
     * 优先级：触发的优先级高于“XCQL应用级触发器”
     * 
     * 注1：只用于查询返回的XCQL。
     * 注2：getCount() 等简单数据结构的也不触发执行。
     * 
     * @param i_AfterRule
     */
    public void setAfterRule(XRule i_AfterRule)
    {
        this.afterRule = this.addPackageImports(i_AfterRule);
    }
    
    
    
    /**
     * 为规则解释器添加默认的包名称及引用类信息
     * 
     * @author      ZhengWei(HY)
     * @createDate  2020-06-02
     * @version     v1.0
     *
     * @param io_XRule
     * @return
     */
    private XRule addPackageImports(XRule io_XRule)
    {
        if ( io_XRule != null )
        {
            if ( !Help.isNull(io_XRule.getValue()) )
            {
                String       v_Package = Help.NVL(io_XRule.getPackage() ,"package org.hy.common.xml.plugins.rules;");
                List<String> v_Imports = new ArrayList<String>();
                
                v_Imports.addAll(io_XRule.getImports());
                v_Imports.add("import java.util.List;");
                v_Imports.add("import java.util.Set;");
                v_Imports.add("import java.util.Map;");
                v_Imports.add("import org.hy.common.Date;");
                
                if ( this.result != null )
                {
                    if ( this.result.getTable() != null )
                    {
                        v_Imports.add("import " + this.result.getTable().getName() + ";");
                    }
                    if ( this.result.getRow() != null )
                    {
                        v_Imports.add("import " + this.result.getRow().getName() + ";");
                    }
                }
                
                v_Imports = Help.toDistinct(v_Imports);
                StringBuilder v_Buffer = new StringBuilder();
                
                v_Buffer.append(v_Package);
                for (String i_Item : v_Imports)
                {
                    v_Buffer.append(i_Item);
                }
                
                v_Imports.add(v_Package);
                v_Buffer.append(StringHelp.replaceAll(io_XRule.getValue() ,v_Imports.toArray(new String[] {}) ,new String[] {""}));
                
                io_XRule.setValue(v_Buffer.toString());
            }
        }
        
        return io_XRule;
    }
    
    
    
    /**
     * 触发执行CQL前的规则引擎。针对CQL参数、占位符的规则引擎
     * 
     * 优先级：触发的优先级高于“XCQL条件”
     * 
     * 注：无入参的不触发执行。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2020-05-27
     * @version     v1.0
     *
     * @param i_XCQLParams
     */
    protected void fireBeforeRule(Object i_XCQLParams)
    {
        if ( this.beforeRule != null && i_XCQLParams != null )
        {
            this.beforeRule.execute(i_XCQLParams);
        }
    }
    
    
    
    /**
     * 触发执行后的规则引擎
     * 
     * 优先级：触发的优先级高于“XCQL应用级触发器”
     * 
     * 注1：无入参的不触发执行。
     * 注2：只用于查询返回的XCQL。
     * 注3：getCount() 等简单数据结构的也不触发执行。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2020-05-26
     * @version     v1.0
     *
     * @param i_XCQLData
     */
    protected void fireAfterRule(XCQLData i_XCQLData)
    {
        if ( this.afterRule != null )
        {
            if ( i_XCQLData.getDatas() != null )
            {
                if ( i_XCQLData.getDatas() instanceof List )
                {
                    this.afterRule.execute(((List<?>)i_XCQLData.getDatas()));
                }
                else if ( i_XCQLData.getDatas() instanceof Set )
                {
                    this.afterRule.execute(((Set<?>)i_XCQLData.getDatas()));
                }
                else
                {
                    // Map 、PartitionMap 、TablePartitionRID 、TablePartitionSet 等对象均将整体对象传入规则引擎
                    this.afterRule.execute(i_XCQLData.getDatas());
                }
            }
            else
            {
                this.afterRule.execute(i_XCQLData);
            }
        }
    }
    
    
    
    @Override
    public int hashCode()
    {
        return this.getObjectID().hashCode();
    }
    
    
    
    @Override
    public boolean equals(Object i_Other)
    {
        if ( i_Other == null )
        {
            return false;
        }
        else if ( this == i_Other )
        {
            return true;
        }
        else if ( i_Other instanceof XCQL )
        {
            return this.getObjectID().equals(((XCQL)i_Other).getObjectID());
        }
        else
        {
            return false;
        }
    }
    
    
    
    @Override
    public int compareTo(XCQL i_XCQL)
    {
        if ( i_XCQL == null )
        {
            return 1;
        }
        else if ( this == i_XCQL )
        {
            return 0;
        }
        else
        {
            return this.getObjectID().compareTo(i_XCQL.getObjectID());
        }
    }
    
}

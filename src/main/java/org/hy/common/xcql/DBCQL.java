package org.hy.common.xcql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hy.common.Help;
import org.hy.common.MethodReflect;
import org.hy.common.PartitionMap;
import org.hy.common.SplitSegment;
import org.hy.common.SplitSegment.InfoType;
import org.hy.common.StringHelp;
import org.hy.common.xml.log.Logger;





/**
 * 图数据库占位符CQL的信息。
 * 
 * 主要对类似如下的CQL信息（我们叫它为:占位符CQL）进行分析后，并根据Java的 "属性类(或叫值对应类)" 转换为真实能被执行的CQL。
 * 
 *  MATCH (f:`源主键`),(t:`源端表` {  <[ tableName: '#tableName' ]>  })
 *  WHERE f.tableID = t.id
 *    AND f.orderBy = "#orderBy"
 *    AND NOT EXISTS { MATCH (f)-[:`主键`]-(t) }
 * CREATE (f)-[:主键]->(t)  ;
 * 
 * 原理是这样的：上述占位符CQL中的 '#tableName' 为占位符。将用 "属性类" getTableName() 方法的返回值进行替换操作。
 * 
 *            1. 当 "属性类" 没有对应的 getTableName() 方法时，
 *               生成的可执行CQL中将不包括 tableName: '#tableName' 的部分。
 * 
 *            2. 当 "属性类" 有对应的 getBeginTime() 方法时，但返回值为 null时，
 *               生成的可执行CQL中将不包括 tableName: '#tableName' 的部分。
 * 
 *            3. '#tableName' 占位符的命名，要符合Java的驼峰命名规则，但首字母可以大写，也可以小写。
 * 
 *            4. "#orderBy" 占位符对应 "属性类" getOrderBy() 方法的返回值类型为基础类型(int、double)时，
 *               不可能为 null 值的情况。即，此占位符在可执行CQL中是必须存在。
 *               如果想其可变，须使用 Integer、Double 类型的返回值类型。
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-05-24
 * @version     v1.0
 */
public class DBCQL implements Serializable
{

    private static final long serialVersionUID = -8245123127082501057L;
    
    private static final Logger              $Logger                     = new Logger(DBCQL.class ,true);
                                                                         
    /** 占位符是什么字符 */
    public  static final String              $Placeholder                = "#";
    
    
    
    /** 未知语法 */
    public final static int                  $DBCQL_TYPE_UNKNOWN         = -1;
                            
    /** 查询语法 */
    public final static int                  $DBCQL_TYPE_MATCH           = 1;
    
    /** 添加语法 */
    public final static int                  $DBCQL_TYPE_CREATE          = 2;
    
    /** 修改语法 */
    public final static int                  $DBCQL_TYPE_SET             = 3;
    
    /** 删除语法 */
    public final static int                  $DBCQL_TYPE_DELETE          = 4;
    
    /** 定义语法 */
    public final static int                  $DBCQL_TYPE_DDL             = 6;
    
    
    
    /** 匹配 <[ ... ]> 的字符串 */
    private final static String              $CQL_Find_Dynamic          = "[ \\s]?<\\[((?!<\\[|\\]>).)*\\]>[ \\s]?";
                                                                        
    /** 匹配 MATCH */
    private final static String              $CQL_Find_Match            = "^( )*[Mm][Aa][Tt][Cc][Hh][ ]+";
                                                                        
    /** 匹配 CREATE */
    private final static String              $CQL_Find_Create           = "( )*[Cc][Rr][Ee][Aa][Tt][Ee][ ]+";
                                                                        
    /** 匹配 CREATE INDEX */
    private final static String              $CQL_Find_CreateIndex      = "^( )*[Cc][Rr][Ee][Aa][Tt][Ee][ ]+[Ii][Nn][Dd][Ee][Xx][ ]+";
                                                                        
    /** 匹配 DROP INDEX */
    private final static String              $CQL_Find_DropIndex        = "^( )*[Dd][Rr][Oo][Pp][ ]+[Ii][Nn][Dd][Ee][Xx][ ]+";
    
    /** 匹配 CREATE CONSTRAINT */
    private final static String              $CQL_Find_CreateConstraint = "^( )*[Cc][Rr][Ee][Aa][Tt][Ee][ ]+[Cc][Oo][Nn][Ss][Tt][Rr][Aa][Ii][Nn][Tt][ ]+";
    
    /** 匹配 DROP CONSTRAINT */
    private final static String              $CQL_Find_DropConstraint   = "^( )*[Dd][Rr][Oo][Pp][ ]+[Cc][Oo][Nn][Ss][Tt][Rr][Aa][Ii][Nn][Tt][ ]+";
    
    /** 匹配 SET */
    private final static String              $CQL_Find_Set              = "[ ]+[Ss][Ee][Tt][ ]+";
                                                                        
    /** 匹配 DELETE */
    private final static String              $CQL_Find_Delete           = "[ ]+[Dd][Ee][Ll][Ee][Tt][Ee][ ]+";
                                                                        
    /** 匹配 REMOVE */
    private final static String              $CQL_Find_Remove           = "[ ]+[Rr][Ee][Mm][Oo][Vv][Ee][ ]+";
    
    
    
    /** 匹配 WHERE <[ */
    private final static String              $CQL_R_WhereDynamic        = "( )*[Ww][Hh][Ee][Rr][Ee][ ]+<\\[";
                                                                       
    /** 匹配 WHERE # */
    private final static String              $CQL_R_WherePlaceho        = "( )*[Ww][Hh][Ee][Rr][Ee][ ]+" + $Placeholder;
                                                                       
    /** 替换 WHERE AND */
    private final static String              $CQL_R_WhereAnd            = "( )*[Ww][Hh][Ee][Rr][Ee][ ]+[Aa][Nn][Dd][ ]+";
                                                                       
    /** 替换 WHERE OR */
    private final static String              $CQL_R_WhereOr             = "( )*[Ww][Hh][Ee][Rr][Ee][ ]+[Oo][Rr][ ]+";
                                                                       
    /** 替换 WHERE Create */
    private final static String              $CQL_R_WhereCreate         = "( )*[Ww][Hh][Ee][Rr][Ee][ ]+[Cc][Rr][Ee][Aa][Tt][Ee][ ]+";
                                                                       
    /** 替换 WHERE Delete */
    private final static String              $CQL_R_WhereDelete         = "( )*[Ww][Hh][Ee][Rr][Ee][ ]+[Dd][Ee][Ll][Ee][Tt][Ee][ ]+";
    
    /** 替换 WHERE Remove */
    private final static String              $CQL_R_WhereRemove         = "( )*[Ww][Hh][Ee][Rr][Ee][ ]+[Rr][Ee][Mm][Oo][Vv][Ee][ ]+";
    
    /** 替换 WHERE Set */
    private final static String              $CQL_R_WhereSet            = "( )*[Ww][Hh][Ee][Rr][Ee][ ]+[Ss][Ee][Tt][ ]+";
    
    /** 替换 WHERE Return */
    private final static String              $CQL_R_WhereReturn         = "( )*[Ww][Hh][Ee][Rr][Ee][ ]+[Rr][Ee][Tt][Uu][Rr][Nn][ ]+";
       
    
    
    /** 数据库中的NULL关键字 */
    private final static String              $NULL               = "NULL";
                                                                 
    private final static Map<String ,String> $ReplaceKeys        = new HashMap<String ,String>();
    
    
    
    static
    {
        $ReplaceKeys.put("\t"  ," ");
        $ReplaceKeys.put("\r"  ," ");
        $ReplaceKeys.put("\n"  ," ");
    }
    
    
    
    /** 数据库连接池组 */
    private DataSourceCQL             dsCQL;
    
    /** 占位符CQL */
    private String                    cqlText;
    
    /** CQL类型 */
    private int                       cqlType;
    
    /** 替换数据库关键字。如，单引号替换成【\'】。默认为：true，即替换 */
    private boolean                   keyReplace;
    
    /** 当this.keyReplace=true时有效。表示个别不替换数据库关键字的占位符。前缀无须#井号 */
    private Set<String>               notKeyReplace;
    
    private DBCQLFill                 dbCQLFill;
    
    /** 是否有WHERE条件后直接跟动态CQL的情况，如 WHERE <[ ... ]> */
    private boolean                   haveWhereDynamic;
    
    /** 通过分析后的分段CQL信息 */
    private List<DBCQL_Split>         segments;
    
    /** 不是占位符的关键字的排除过滤。区分大小字。前缀无须冒号 */
    private Set<String>               notPlaceholders;
    
    /** 占位符取值条件 */
    private Map<String ,DBConditions> conditions;
    
    /**
     * 是否默认为NULL值写入到数据库。针对所有占位符做的统一设置。
     * 
     * 当 this.defaultNull = true 时，任何类型的值为null对象时，均向以NULL值写入到数据库。
     * 当 this.defaultNull = false 时，
     *      1. String 类型的值，按 "" 空字符串写入到数据库 或 拼接成CQL语句
     *      2. 其它类型的值，以NULL值写入到数据库。
     * 
     * 默认为：false。
     */
    private boolean                   defaultNull;
    
    
    
    /**
     * 构造器
     */
    public DBCQL()
    {
        this.cqlText          = "";
        this.cqlType          = $DBCQL_TYPE_UNKNOWN;
        this.haveWhereDynamic = false;
        this.segments         = new ArrayList<DBCQL_Split>();
        this.conditions       = new HashMap<String ,DBConditions>();
        this.defaultNull      = false;
        this.setNotPlaceholders("MI,SS,mi,ss");
        this.setKeyReplace(true);
    }
    
    
    
    /**
     * 构造器
     * 
     * @param i_CQLText  完整的原始CQL文本
     */
    public DBCQL(String i_CQLText)
    {
        this();
        this.setCqlText(i_CQLText);
    }
    
    
    
    /**
     * 分析CQL（私有）
     * 
     * @return
     */
    private void parser()
    {
        if ( Help.isNull(this.cqlText) )
        {
            return;
        }
        
        this.parser_CQLType();
        this.parser_WhereDynamic();
        
        // 匹配 <[ ... ]> 的字符串
        List<SplitSegment> v_Segments = StringHelp.Split($CQL_Find_Dynamic ,this.cqlText);
        for (SplitSegment v_SplitSegment : v_Segments)
        {
            DBCQL_Split v_DBCQL_Segment = new DBCQL_Split(v_SplitSegment);
            
            String v_Info = v_DBCQL_Segment.getInfo();
            v_Info = v_Info.replaceFirst("<\\[" ,"");
            v_Info = v_Info.replaceFirst("\\]>" ,"");
            
            v_DBCQL_Segment.setInfo(v_Info);
            v_DBCQL_Segment.parsePlaceholders();
            
            this.segments.add(v_DBCQL_Segment);
        }
    }
    
    
    
    /**
     * 识别CQL语句的类型
     */
    private void parser_CQLType()
    {
        Pattern v_Pattern = null;
        Matcher v_Matcher = null;
        
        v_Pattern = Pattern.compile($CQL_Find_CreateIndex);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.cqlType = $DBCQL_TYPE_DDL;
            return;
        }
        
        v_Pattern = Pattern.compile($CQL_Find_DropIndex);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.cqlType = $DBCQL_TYPE_DDL;
            return;
        }
        
        v_Pattern = Pattern.compile($CQL_Find_CreateConstraint);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.cqlType = $DBCQL_TYPE_DDL;
            return;
        }
        
        v_Pattern = Pattern.compile($CQL_Find_DropConstraint);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.cqlType = $DBCQL_TYPE_DDL;
            return;
        }
        
        v_Pattern = Pattern.compile($CQL_Find_Delete);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.cqlType = $DBCQL_TYPE_DELETE;
            return;
        }
        
        v_Pattern = Pattern.compile($CQL_Find_Remove);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.cqlType = $DBCQL_TYPE_DELETE;
            return;
        }
        
        v_Pattern = Pattern.compile($CQL_Find_Set);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.cqlType = $DBCQL_TYPE_SET;
            return;
        }
        
        v_Pattern = Pattern.compile($CQL_Find_Create);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.cqlType = $DBCQL_TYPE_CREATE;
            return;
        }
        
        v_Pattern = Pattern.compile($CQL_Find_Match);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.cqlType = $DBCQL_TYPE_MATCH;
            return;
        }
    }
    
    
    
    /**
     * 是否有WHERE条件后直接跟动态CQL的情况，如 WHERE <[ ... ]>
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-05-12
     * @version     v1.0
     *
     */
    private void parser_WhereDynamic()
    {
        Pattern v_Pattern = null;
        Matcher v_Matcher = null;
        
        v_Pattern = Pattern.compile($CQL_R_WhereDynamic);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.haveWhereDynamic = true;
            return;
        }
        
        v_Pattern = Pattern.compile($CQL_R_WherePlaceho);
        v_Matcher = v_Pattern.matcher(this.cqlText);
        if ( v_Matcher.find() )
        {
            this.haveWhereDynamic = true;
        }
    }
    
    
    
    /**
     * 当有WHERE条件后直接跟动态CQL的情况进行处理。
     * 支持如下场景的动态CQL的解释，同时无须写成 WHERE 1 = 1 的固定形式。
     * 
     *    场景01：  MATCH  (n)
     *             WHERE
     *          <[   AND  n.字段A = '#占位符A'  ]>
     *          <[   AND  n.字段B = '#占位符B'  ]>
     * 
     *    场景02：  MATCH  (n)
     *             WHERE
     *          <[        n.字段A = '#占位符A'  ]>
     *          <[    OR  n.字段B = '#占位符B'  ]>
     * 
     *    场景03：  MATCH  (n)
     *             WHERE
     *          <[   AND  n.字段A = '#占位符A'  ]>
     *          <[   AND  n.字段B = '#占位符B'  ]>
     *            RETURN  n
     * 
     *    场景04：  MATCH  (n)
     *             WHERE
     *          <[   AND  字段A = '#占位符A'  ]>
     *          <[   AND  字段B = '#占位符B'  ]>
     *            CREATE  n
     * 
     *    场景05：  MATCH  (n)
     *             WHERE
     *          <[   AND  字段A = '#占位符A'  ]>
     *          <[   AND  字段B = '#占位符B'  ]>
     *               SET  n
     * 
     *    场景06：  MATCH  (n)
     *             WHERE
     *          <[   AND  字段A = '#占位符A'  ]>
     *          <[   AND  字段B = '#占位符B'  ]>
     *            DELETE  n
     *
     *    场景07：  MATCH  (n)
     *             WHERE
     *          <[   AND  字段A = '#占位符A'  ]>
     *          <[   AND  字段B = '#占位符B'  ]>
     *            REMOVE  (n {字段A: '#占位符A'})
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-05-12
     * @version     v1.0
     *
     * @param i_CQL
     * @return
     */
    private String whereDynamic(String i_CQL)
    {
        if ( !this.haveWhereDynamic )
        {
            return i_CQL;
        }
        
        String  v_CQL     = i_CQL.trim();
        Pattern v_Pattern = null;
        Matcher v_Matcher = null;
        
        if ( v_CQL.toUpperCase().endsWith(" WHERE") )
        {
            v_CQL = v_CQL.substring(0 ,v_CQL.length() - 6);
        }
        
        v_Pattern = Pattern.compile($CQL_R_WhereAnd);
        v_Matcher = v_Pattern.matcher(v_CQL);
        if ( v_Matcher.find() )
        {
            v_CQL = v_Matcher.replaceAll(" WHERE ");
        }
        
        v_Pattern = Pattern.compile($CQL_R_WhereOr);
        v_Matcher = v_Pattern.matcher(v_CQL);
        if ( v_Matcher.find() )
        {
            v_CQL = v_Matcher.replaceAll(" WHERE ");
        }
        
        v_Pattern = Pattern.compile($CQL_R_WhereCreate);
        v_Matcher = v_Pattern.matcher(v_CQL);
        if ( v_Matcher.find() )
        {
            v_CQL = v_Matcher.replaceAll(" CREATE ");
        }
        
        v_Pattern = Pattern.compile($CQL_R_WhereDelete);
        v_Matcher = v_Pattern.matcher(v_CQL);
        if ( v_Matcher.find() )
        {
            v_CQL = v_Matcher.replaceAll(" DELETE ");
        }
        
        v_Pattern = Pattern.compile($CQL_R_WhereRemove);
        v_Matcher = v_Pattern.matcher(v_CQL);
        if ( v_Matcher.find() )
        {
            v_CQL = v_Matcher.replaceAll(" REMOVE ");
        }
        
        v_Pattern = Pattern.compile($CQL_R_WhereSet);
        v_Matcher = v_Pattern.matcher(v_CQL);
        if ( v_Matcher.find() )
        {
            v_CQL = v_Matcher.replaceAll(" SET ");
        }
        
        v_Pattern = Pattern.compile($CQL_R_WhereReturn);
        v_Matcher = v_Pattern.matcher(v_CQL);
        if ( v_Matcher.find() )
        {
            v_CQL = v_Matcher.replaceAll(" RETURN ");
        }
        
        return v_CQL;
    }
    
    
    
    /**
     * 获取：是否有WHERE条件后直接跟动态CQL的情况，如 WHERE <[ ... ]>
     */
    public boolean isHaveWhereDynamic()
    {
        return haveWhereDynamic;
    }
    
    
    
    /**
     * 填充或设置占位符CQL
     * 
     * @param i_CQLText
     */
    public synchronized void setCqlText(String i_CQLText)
    {
        this.cqlText = StringHelp.replaceAll(Help.NVL(i_CQLText).trim() ,$ReplaceKeys);
        
        if ( this.segments == null )
        {
            this.segments = new ArrayList<DBCQL_Split>();
        }
        else
        {
            this.segments.clear();
        }
        
        this.parser();
    }
    
    
    
    /**
     * 获取占位符CQL
     * 
     * @return
     */
    public synchronized String getCqlText()
    {
        return cqlText;
    }
    
    
    
    /**
     * 获取可执行的CQL语句，并按 i_Obj 填充有数值。
     * 
     * 入参类型是Map时，在处理NULL与入参类型是Object，是不同的。
     *   1. Map填充为""空的字符串。
     *   2. Object填充为 "NULL" ，可以支持空值针的写入。
     * 
     *   但上方两种均可以通过配置<condition><name>占位符名称<name></condition>，向数据库写入空值针。
     * 
     * @param i_Obj
     * @return
     */
    public String getCQL(Object i_Obj)
    {
        return this.getCQL(i_Obj ,this.dsCQL);
    }
    
    
    
    /**
     * 获取可执行的CQL语句，并按 i_Obj 填充有数值。
     * 
     * 入参类型是Map时，在处理NULL与入参类型是Object，是不同的。
     *   1. Map填充为""空的字符串。
     *   2. Object填充为 "NULL" ，可以支持空值针的写入。
     * 
     *   但上方两种均可以通过配置<condition><name>占位符名称<name></condition>，向数据库写入空值针。
     * 
     * @param i_Obj
     * @param i_DSG  数据库连接池组。可为空或NULL
     * @return
     */
    @SuppressWarnings("unchecked")
    public String getCQL(Object i_Obj ,DataSourceCQL i_DSCQL)
    {
        if ( i_Obj == null )
        {
            return null;
        }
        
        if ( Help.isNull(this.segments) )
        {
            return this.cqlText;
        }
        
        if ( i_Obj instanceof Map )
        {
            return this.getCQL((Map<String ,?>)i_Obj ,i_DSCQL);
        }
        
        String                v_DBType  = null;
        StringBuilder         v_CQL     = new StringBuilder();
        Iterator<DBCQL_Split> v_Ierator = this.segments.iterator();
        
        
        while ( v_Ierator.hasNext() )
        {
            DBCQL_Split                   v_DBCQL_Segment = v_Ierator.next();
            PartitionMap<String ,Integer> v_Placeholders  = v_DBCQL_Segment.getPlaceholders();
            
            if ( Help.isNull(v_Placeholders) )
            {
                v_CQL.append(v_DBCQL_Segment.getInfo());
            }
            else
            {
                Iterator<String> v_IterPlaceholders = v_Placeholders.keySet().iterator();
                String           v_Info             = v_DBCQL_Segment.getInfo();
                int              v_ReplaceCount     = 0;
                
                // 不再区分 $DBCQL_TYPE_INSERT 类型，使所有的CQL类型均采有相同的占位符填充逻辑。ZhengWei(HY) Edit 2018-06-06
                while ( v_IterPlaceholders.hasNext() )
                {
                    String        v_PlaceHolder   = v_IterPlaceholders.next();
                    MethodReflect v_MethodReflect = null;
                    
                    // 排除不是占位符的变量，但它的形式可能是占位符的形式。ZhengWei(HY) Add 2018-06-14
                    if ( this.notPlaceholders.contains(v_PlaceHolder) )
                    {
                        v_ReplaceCount++;
                        continue;
                    }
                    
                    /*
                    在实现全路径的解释功能之前的老方法  ZhengWei(HY) Del 2015-12-10
                    Method v_Method = MethodReflect.getGetMethod(i_Obj.getClass() ,v_PlaceHolder ,true);
                    */
                    
                    // 可实现xxx.yyy.www(或getXxx.getYyy.getWww)全路径的解释  ZhengWei(HY) Add 2015-12-10
                    try
                    {
                        v_MethodReflect = new MethodReflect(i_Obj ,v_PlaceHolder ,true ,MethodReflect.$NormType_Getter);
                    }
                    catch (Exception exce)
                    {
                        // 有些:xx占位符可能找到对应Java的Getter方法，所以忽略。 ZhengWei(HY) Add 2-16-09-29
                        // Nothing.
                    }
                    
                    Object       v_GetterValue    = null;
                    DBConditions v_ConditionGroup = null;
                    boolean      v_IsCValue       = false;
                    try
                    {
                        if ( v_MethodReflect != null )
                        {
                            v_ConditionGroup = Help.getValueIgnoreCase(this.conditions ,v_PlaceHolder);
                            if ( v_ConditionGroup != null )
                            {
                                // 占位符取值条件  ZhengWei(HY) Add 2018-08-10
                                v_GetterValue = v_ConditionGroup.getValue(i_Obj ,false);
                                v_IsCValue    = true;
                            }
                            else
                            {
                                v_GetterValue = v_MethodReflect.invoke();
                            }
                        }
                        else
                        {
                            // 全局占位符 ZhengWei(HY) Add 2019-03-06
                            v_GetterValue = Help.getValueIgnoreCase(DBCQLStaticParams.getInstance() ,v_PlaceHolder);
                        }
                    }
                    catch (Exception exce)
                    {
                        $Logger.error(exce);
                        throw new RuntimeException(exce.getMessage());
                    }
                    
                    try
                    {
                        // getter 方法有返回值时
                        if ( v_GetterValue != null )
                        {
                            if ( MethodReflect.class.equals(v_GetterValue.getClass()) )
                            {
                                boolean v_IsReplace = false;
                                
                                // 这里循环的原因是：每次((MethodReflect)v_GetterValue).invoke()执行后的返回值v_MRValue都可能不一样。
                                while ( v_Info.indexOf($Placeholder + v_PlaceHolder) >= 0 )
                                {
                                    // 可实现CQL中的占位符，通过Java动态(或有业务时间逻辑的)填充值。 ZhengWei(HY) Add 2016-03-18
                                    Object v_MRValue = ((MethodReflect)v_GetterValue).invoke();
                                    
                                    if ( v_MRValue != null )
                                    {
                                        if ( v_IsCValue )
                                        {
                                            v_Info = this.dbCQLFill.onlyFillFirst(v_Info ,v_PlaceHolder ,v_MRValue.toString() ,v_DBType);
                                        }
                                        else
                                        {
                                            v_Info = this.dbCQLFill.fillFirst(v_Info ,v_PlaceHolder ,v_MRValue.toString() ,v_DBType);
                                        }
                                        v_IsReplace = true;
                                    }
                                    else
                                    {
                                        String v_Value = Help.toObject(((MethodReflect)v_GetterValue).getReturnType()).toString();
                                        v_Info = this.dbCQLFill.fillAll(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                                        v_IsReplace = false;  // 为了支持动态占位符，这里设置为false
                                        // 同时也替换占位符，可对不是动态占位符的情况，也初始化值。  ZhengWei(HY) 2018-06-06
                                        
                                        break;
                                    }
                                }
                                
                                if ( v_IsReplace )
                                {
                                    v_ReplaceCount++;
                                }
                            }
                            else
                            {
                                if ( v_IsCValue )
                                {
                                    v_Info = this.dbCQLFill.onlyFillAll(v_Info ,v_PlaceHolder ,v_GetterValue.toString() ,v_DBType);
                                }
                                else
                                {
                                    v_Info = this.dbCQLFill.fillAll(v_Info ,v_PlaceHolder ,v_GetterValue.toString() ,v_DBType);
                                }
                                v_ReplaceCount++;
                            }
                        }
                        // 当占位符对应属性值为NULL时的处理
                        else
                        {
                            String v_Value = null;
                            if ( v_ConditionGroup != null || this.defaultNull )
                            {
                                // 占位符取值条件。可实现NULL值写入到数据库的功能  ZhengWei(HY) Add 2018-08-10
                                v_Value = $NULL;
                                v_Info  = this.dbCQLFill.fillAllMark(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                            }
                            else if ( v_MethodReflect == null )
                            {
                                v_Value = $NULL;
                                v_Info  = this.dbCQLFill.fillAllMark(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                            }
                            else
                            {
                                Class<?> v_ReturnType = v_MethodReflect.getReturnType();
                                if ( v_ReturnType == null ||  v_ReturnType == String.class )
                                {
                                    v_Value = "";
                                }
                                else
                                {
                                    v_Value = $NULL;
                                    v_Info  = this.dbCQLFill.fillAllMark(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                                }
                                
                                // 2018-11-02 Del  废除默认值填充方式
                                // v_Value = Help.toObject(v_MethodReflect.getReturnType()).toString();
                            }
                            
                            // 这里必须再执行一次填充。因为第一次为 fillMark()，本次为 fillAll() 方法
                            v_Info = this.dbCQLFill.fillAll(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                            
                            // v_ReplaceCount++; 此处不要++，这样才能实现动态占位符的功能。
                            // 上面的代码同时也替换占位符，可对不是动态占位符的情况，也初始化值。  ZhengWei(HY) 2018-06-06
                        }
                    }
                    catch (Exception exce)
                    {
                        $Logger.error(exce);
                    }
                    
                    if ( v_MethodReflect != null )
                    {
                        v_MethodReflect.clearDestroy();
                        v_MethodReflect = null;
                    }
                }
                
                if ( InfoType.$TextInfo == v_DBCQL_Segment.getInfoType() )
                {
                    v_CQL.append(v_Info);
                }
                else if ( v_ReplaceCount == v_DBCQL_Segment.getPlaceholderSize() )
                {
                    v_CQL.append(v_Info);
                }
            }
        }
        
        // 2018-03-22  优化：完善安全检查防止CQL注入，将'--形式的CQL放在整体CQL来判定。
        String v_CQLRet = v_CQL.toString();
        v_CQLRet = whereDynamic(v_CQLRet);
        return v_CQLRet;
    }
    
    
    
    /**
     * 获取可执行的CQL语句，并按 Map<String ,Object> 填充有数值。
     * 
     * Map.key  即为占位符。
     *     2016-03-16 将不再区分大小写的模式配置参数。
     * 
     * 入参类型是Map时，在处理NULL与入参类型是Object，是不同的。
     *   1. Map填充为""空的字符串。
     *   2. Object填充为 "NULL" ，可以支持空值针的写入。
     * 
     *   但上方两种均可以通过配置<condition><name>占位符名称<name></condition>，向数据库写入空值针。
     * 
     * @param i_Obj
     * @return
     */
    public String getCQL(Map<String ,?> i_Values)
    {
        return this.getCQL(i_Values ,this.dsCQL);
    }
    
    
    
    /**
     * 获取可执行的CQL语句，并按 Map<String ,Object> 填充有数值。
     * 
     * Map.key  即为占位符。
     *     2016-03-16 将不再区分大小写的模式配置参数。
     * 
     * 入参类型是Map时，在处理NULL与入参类型是Object，是不同的。
     *   1. Map填充为""空的字符串。
     *   2. Object填充为 "NULL" ，可以支持空值针的写入。
     * 
     *   但上方两种均可以通过配置<condition><name>占位符名称<name></condition>，向数据库写入空值针。
     * 
     * @param i_Obj
     * @param i_DSCQL  数据库连接信息。可为空或NULL
     * @return
     */
    public String getCQL(Map<String ,?> i_Values ,DataSourceCQL i_DSCQL)
    {
        if ( i_Values == null )
        {
            return null;
        }
        
        if ( Help.isNull(this.segments) )
        {
            return this.cqlText;
        }
        
        String                v_DBType  = null;
        StringBuilder         v_CQL     = new StringBuilder();
        Iterator<DBCQL_Split> v_Ierator = this.segments.iterator();

        // 不再区分 $DBCQL_TYPE_INSERT 类型，使所有的CQL类型均采有相同的占位符填充逻辑。ZhengWei(HY) Edit 2018-06-06
        while ( v_Ierator.hasNext() )
        {
            DBCQL_Split                   v_DBCQL_Segment = v_Ierator.next();
            PartitionMap<String ,Integer> v_Placeholders  = v_DBCQL_Segment.getPlaceholders();
            
            if ( Help.isNull(v_Placeholders) )
            {
                v_CQL.append(v_DBCQL_Segment.getInfo());
            }
            else
            {
                Iterator<String> v_IterPlaceholders = v_Placeholders.keySet().iterator();
                String           v_Info             = v_DBCQL_Segment.getInfo();
                int              v_ReplaceCount     = 0;
                
                while ( v_IterPlaceholders.hasNext() )
                {
                    String v_PlaceHolder = v_IterPlaceholders.next();
                    
                    // 排除不是占位符的变量，但它的形式可能是占位符的形式。ZhengWei(HY) Add 2018-06-14
                    if ( this.notPlaceholders.contains(v_PlaceHolder) )
                    {
                        v_ReplaceCount++;
                        continue;
                    }
                    
                    try
                    {
                        Object       v_MapValue       = null;
                        DBConditions v_ConditionGroup = Help.getValueIgnoreCase(this.conditions ,v_PlaceHolder);
                        boolean      v_IsCValue       = false;
                        if ( v_ConditionGroup != null )
                        {
                            // 占位符取值条件  ZhengWei(HY) Add 2018-08-10
                            v_MapValue = v_ConditionGroup.getValue(i_Values ,false);
                            v_IsCValue = true;
                        }
                        else
                        {
                            v_MapValue = MethodReflect.getMapValue(i_Values ,v_PlaceHolder);
                        }
                        
                        // 全局占位符 ZhengWei(HY) Add 2019-03-06
                        if ( v_MapValue == null )
                        {
                            v_MapValue = Help.getValueIgnoreCase(DBCQLStaticParams.getInstance() ,v_PlaceHolder);
                        }
                        
                        if ( v_MapValue != null )
                        {
                            if ( MethodReflect.class.equals(v_MapValue.getClass()) )
                            {
                                boolean v_IsReplace = false;
                                
                                while ( v_Info.indexOf($Placeholder + v_PlaceHolder) >= 0 )
                                {
                                    // 可实现CQL中的占位符，通过Java动态(或有业务时间逻辑的)填充值。 ZhengWei(HY) Add 2016-03-18
                                    Object v_GetterValue = ((MethodReflect)v_MapValue).invoke();
                                    
                                    // getter 方法有返回值时
                                    if ( v_GetterValue != null )
                                    {
                                        if ( v_IsCValue )
                                        {
                                            v_Info = this.dbCQLFill.onlyFillFirst(v_Info ,v_PlaceHolder ,v_GetterValue.toString() ,v_DBType);
                                        }
                                        else
                                        {
                                            v_Info = this.dbCQLFill.fillFirst(v_Info ,v_PlaceHolder ,v_GetterValue.toString() ,v_DBType);
                                        }
                                        v_IsReplace = true;
                                    }
                                    else
                                    {
                                        String v_Value = null;
                                        if ( v_ConditionGroup != null || this.defaultNull )
                                        {
                                            // 占位符取值条件。可实现NULL值写入到数据库的功能  ZhengWei(HY) Add 2018-08-10
                                            v_Value = $NULL;
                                            v_Info = this.dbCQLFill.fillAllMark(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                                        }
                                        else
                                        {
                                            Class<?> v_ReturnType = ((MethodReflect)v_MapValue).getReturnType();
                                            if ( v_ReturnType == null ||  v_ReturnType == String.class )
                                            {
                                                v_Value = "";
                                            }
                                            else
                                            {
                                                v_Value = $NULL;
                                                v_Info = this.dbCQLFill.fillAllMark(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                                            }
                                            
                                            // 2018-11-02 Del  废除默认值填充方式
                                            // v_Value = Help.toObject(((MethodReflect)v_MapValue).getReturnType()).toString();
                                        }
                                        
                                        // 这里必须再执行一次填充。因为第一次为 fillMark()，本次为 fillAll() 方法
                                        v_Info = this.dbCQLFill.fillAll(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                                        
                                        v_IsReplace = false;  // 为了支持动态占位符，这里设置为false
                                        // 同时也替换占位符，可对不是动态占位符的情况，也初始化值。  ZhengWei(HY) 2018-06-06
                                        
                                        break;
                                    }
                                }
                                
                                if ( v_IsReplace )
                                {
                                    v_ReplaceCount++;
                                }
                            }
                            else
                            {
                                if ( v_IsCValue )
                                {
                                    v_Info = this.dbCQLFill.onlyFillAll(v_Info ,v_PlaceHolder ,v_MapValue.toString() ,v_DBType);
                                }
                                else
                                {
                                    v_Info = this.dbCQLFill.fillAll(v_Info ,v_PlaceHolder ,v_MapValue.toString() ,v_DBType);
                                }
                                v_ReplaceCount++;
                            }
                        }
                        else
                        {
                            // 对于没有<[ ]>可选分段的CQL
                            if ( 1 == this.segments.size() )
                            {
                                if ( v_ConditionGroup != null || this.defaultNull )
                                {
                                    // 占位符取值条件。可实现NULL值写入到数据库的功能  ZhengWei(HY) Add 2018-08-10
                                    String v_Value = $NULL;
                                    v_Info = this.dbCQLFill.fillAllMark(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                                    v_Info = this.dbCQLFill.fillAll    (v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                                }
                                else
                                {
                                    v_Info = this.dbCQLFill.fillSpace(v_Info ,v_PlaceHolder);
                                }
                                v_ReplaceCount++;
                            }
                            else
                            {
                                String v_Value = null;
                                if ( v_ConditionGroup != null || this.defaultNull )
                                {
                                    // 占位符取值条件。可实现NULL值写入到数据库的功能  ZhengWei(HY) Add 2018-08-10
                                    v_Value = $NULL;
                                    v_Info = this.dbCQLFill.fillAllMark(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                                }
                                else
                                {
                                    v_Value = "";
                                }
                                
                                // 这里必须再执行一次填充。因为第一次为 fillMark()，本次为 fillAll() 方法
                                v_Info = this.dbCQLFill.fillAll(v_Info ,v_PlaceHolder ,v_Value ,v_DBType);
                            }
                        }
                    }
                    catch (Exception exce)
                    {
                        $Logger.error(exce);
                    }
                }
                
                if ( InfoType.$TextInfo == v_DBCQL_Segment.getInfoType() )
                {
                    v_CQL.append(v_Info);
                }
                else if ( v_ReplaceCount == v_DBCQL_Segment.getPlaceholderSize() )
                {
                    v_CQL.append(v_Info);
                }
            }
        }
        
        // 2018-03-22  优化：完善安全检查防止CQL注入，将'--形式的CQL放在整体CQL来判定。
        String v_CQLRet = v_CQL.toString();
        v_CQLRet = whereDynamic(v_CQLRet);
        return v_CQLRet;
    }
    
    
    
    /**
     * 获取可执行的CQL语句，无填充项的情况。
     * 
     * @return
     */
    public String getCQL()
    {
        return this.getCQL(this.dsCQL);
    }
    
    
    
    /**
     * 获取可执行的CQL语句，无填充项的情况。
     * 
     * @param i_DSCQL  数据库连接信息。可为空或NULL
     * @return
     */
    public String getCQL(DataSourceCQL i_DSCQL)
    {
        if ( Help.isNull(this.segments) )
        {
            return this.cqlText;
        }
        
        String                v_DBType  = null;
        StringBuilder         v_CQL     = new StringBuilder();
        Iterator<DBCQL_Split> v_Ierator = this.segments.iterator();

        // 不再区分 $DBCQL_TYPE_INSERT 类型，使所有的CQL类型均采有相同的占位符填充逻辑。ZhengWei(HY) Edit 2018-06-06
        while ( v_Ierator.hasNext() )
        {
            DBCQL_Split                   v_DBCQL_Segment = v_Ierator.next();
            PartitionMap<String ,Integer> v_Placeholders  = v_DBCQL_Segment.getPlaceholders();
            
            if ( Help.isNull(v_Placeholders) )
            {
                v_CQL.append(v_DBCQL_Segment.getInfo());
            }
            else
            {
                Iterator<String> v_IterPlaceholders = v_Placeholders.keySet().iterator();
                String           v_Info             = v_DBCQL_Segment.getInfo();
                int              v_ReplaceCount     = 0;
                
                while ( v_IterPlaceholders.hasNext() )
                {
                    String v_PlaceHolder = v_IterPlaceholders.next();
                    
                    // 排除不是占位符的变量，但它的形式可能是占位符的形式。ZhengWei(HY) Add 2018-06-14
                    if ( this.notPlaceholders.contains(v_PlaceHolder) )
                    {
                        v_ReplaceCount++;
                        continue;
                    }
                    
                    try
                    {
                        // 全局占位符 ZhengWei(HY) Add 2019-03-06
                        Object v_MapValue = Help.getValueIgnoreCase(DBCQLStaticParams.getInstance() ,v_PlaceHolder);
                        
                        if ( v_MapValue != null )
                        {
                            if ( MethodReflect.class.equals(v_MapValue.getClass()) )
                            {
                                boolean v_IsReplace = false;
                                
                                while ( v_Info.indexOf($Placeholder + v_PlaceHolder) >= 0 )
                                {
                                    // 可实现CQL中的占位符，通过Java动态(或有业务时间逻辑的)填充值。 ZhengWei(HY) Add 2016-03-18
                                    Object v_GetterValue = ((MethodReflect)v_MapValue).invoke();
                                    
                                    // getter 方法有返回值时
                                    if ( v_GetterValue != null )
                                    {
                                        v_Info = this.dbCQLFill.fillFirst(v_Info ,v_PlaceHolder ,v_GetterValue.toString() ,v_DBType);
                                        v_IsReplace = true;
                                    }
                                    // else
                                    // {
                                        // 因为没有执行参数，所以不做任何替换  2019-03-13
                                    // }
                                }
                                
                                if ( v_IsReplace )
                                {
                                    v_ReplaceCount++;
                                }
                            }
                            else
                            {
                                v_Info = this.dbCQLFill.fillAll(v_Info ,v_PlaceHolder ,v_MapValue.toString() ,v_DBType);
                                v_ReplaceCount++;
                            }
                        }
                        // else
                        // {
                            // 因为没有执行参数，所以不做任何替换  2019-03-13
                        // }
                    }
                    catch (Exception exce)
                    {
                        $Logger.error(exce);
                    }
                }
                
                if ( InfoType.$TextInfo == v_DBCQL_Segment.getInfoType() )
                {
                    v_CQL.append(v_Info);
                }
                else if ( v_ReplaceCount == v_DBCQL_Segment.getPlaceholderSize() )
                {
                    v_CQL.append(v_Info);
                }
            }
        }
        
        // 2018-03-22  优化：完善安全检查防止CQL注入，将'--形式的CQL放在整体CQL来判定。
        String v_CQLRet = v_CQL.toString();
        v_CQLRet = whereDynamic(v_CQLRet);
        return v_CQLRet;
    }
    
    
    
    public int getCQLType()
    {
        return cqlType;
    }
    
    
    
    /**
     * 获取：替换数据库关键字。如，单引号替换成两个单引号。默认为：true，即替换
     */
    public boolean isKeyReplace()
    {
        return keyReplace;
    }


    
    /**
     * 设置：替换数据库关键字。如，单引号替换成两个单引号。默认为：true，即替换
     * 
     * 采用类似工厂方法构造 DBCQLFill，惟一的目的就是为了生成CQL时，减少IF判断，提高速度。
     * 
     * @param i_KeyReplace
     */
    public void setKeyReplace(boolean i_KeyReplace)
    {
        if ( i_KeyReplace )
        {
            this.dbCQLFill = DBCQLFillKeyReplace.getInstance(this.notKeyReplace);
        }
        else
        {
            this.dbCQLFill = DBCQLFillDefault.getInstance();
        }
        
        this.keyReplace = i_KeyReplace;
    }
    
    
    
    /**
     * 获取：不是占位符的关键字的排除过滤。区分大小字。前缀无须冒号
     */
    public Set<String> getNotPlaceholderSet()
    {
        return notPlaceholders;
    }
    
    
    
    /**
     * 获取：不是占位符的关键字的排除过滤。区分大小字。前缀无须冒号
     * 
     * @param i_NotPlaceholders
     */
    public void setNotPlaceholderSet(Set<String> i_NotPlaceholders)
    {
        this.notPlaceholders = i_NotPlaceholders;
    }
    

    
    /**
     * 设置：不是占位符的关键字的排除过滤。区分大小字。前缀无须冒号。
     * 
     * @param i_NotPlaceholders  多个间用,逗号分隔
     */
    public void setNotPlaceholders(String i_NotPlaceholders)
    {
        this.notPlaceholders = new HashSet<String>();
        
        String [] v_Arr = i_NotPlaceholders.split(",");
        if ( !Help.isNull(v_Arr) )
        {
            for (String v_Placeholder : v_Arr)
            {
                this.notPlaceholders.add(v_Placeholder.trim());
            }
        }
    }
    
    
    
    /**
     * 获取：当this.keyReplace=true时有效。表示个别不替换数据库关键字的占位符。前缀无须冒号
     */
    public Set<String> getNotKeyReplaceSet()
    {
        return notKeyReplace;
    }
    

    
    /**
     * 设置：当this.keyReplace=true时有效。表示个别不替换数据库关键字的占位符。前缀无须冒号
     * 
     * @param notKeyReplace
     */
    public void setNotKeyReplaceSet(Set<String> notKeyReplace)
    {
        this.notKeyReplace = notKeyReplace;
    }
    
    
    
    /**
     * 设置：当this.keyReplace=true时有效。表示个别不替换数据库关键字的占位符。前缀无须冒号。
     * 
     * @param notKeyReplaces  多个间用,逗号分隔
     */
    public void setNotKeyReplaces(String notKeyReplaces)
    {
        this.notKeyReplace = new HashSet<String>();
        
        String [] v_Arr = notKeyReplaces.split(",");
        if ( !Help.isNull(v_Arr) )
        {
            for (String v_NotKeyReplace : v_Arr)
            {
                this.notKeyReplace.add(v_NotKeyReplace.trim());
            }
        }
        
        this.setKeyReplace(this.isKeyReplace());
    }
    


    /**
     * 获取：占位符取值条件
     */
    public Map<String ,DBConditions> getConditions()
    {
        return conditions;
    }
    
    
    
    /**
     * 设置：占位符取值条件
     * 
     * @param conditions
     */
    public void setConditions(Map<String ,DBConditions> conditions)
    {
        this.conditions = conditions;
    }
    
    
    
    /**
     * 添加占位符取值条件
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-08-10
     * @version     v1.0
     *
     * @param i_Condition   条件
     */
    public void addCondition(DBCondition i_Condition)
    {
        if ( i_Condition == null || Help.isNull(i_Condition.getName()) )
        {
            return;
        }
        
        DBConditions v_ConditionGroup = new DBConditions();
        v_ConditionGroup.addCondition(i_Condition);
        
        this.addCondition(i_Condition.getName() ,v_ConditionGroup);
    }
    
    
    
    /**
     * 添加占位符取值的条件组
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-01-20
     * @version     v1.0
     *
     * @param i_ConditionGroup   条件组
     */
    public void addCondition(DBConditions i_ConditionGroup)
    {
        if ( i_ConditionGroup == null
          || i_ConditionGroup.size() < 0
          || Help.isNull(i_ConditionGroup.getName()) )
        {
            return;
        }
        
        i_ConditionGroup.setName(i_ConditionGroup.getName());
        
        this.conditions.put(i_ConditionGroup.getName() ,i_ConditionGroup);
    }
    
    
    
    /**
     * 添加占位符取值的条件组
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-01-19
     * @version     v1.0
     *
     * @param i_PlaceholderName  占位符名称（不含前缀的冒号:）
     * @param i_ConditionGroup   条件组
     */
    public void addCondition(String i_PlaceholderName ,DBConditions i_ConditionGroup)
    {
        if ( Help.isNull(i_PlaceholderName)
          || i_ConditionGroup == null
          || i_ConditionGroup.size() < 0 )
        {
            return;
        }
        
        i_ConditionGroup.setName(i_PlaceholderName);
        
        this.conditions.put(i_PlaceholderName ,i_ConditionGroup);
    }
    

    
    /**
     * 获取：是否默认为NULL值写入到数据库。针对所有占位符做的统一设置。
     * 
     * 当 this.defaultNull = true 时，任何类型的值为null对象时，均向以NULL值写入到数据库。
     * 当 this.defaultNull = false 时，
     *      1. String 类型的值，按 "" 空字符串写入到数据库 或 拼接成CQL语句
     *      2. 其它类型的值，以NULL值写入到数据库。
     * 
     * 默认为：false。
     */
    public boolean isDefaultNull()
    {
        return defaultNull;
    }

    
    
    /**
     * 设置：是否默认为NULL值写入到数据库。针对所有占位符做的统一设置。
     * 
     * 当 this.defaultNull = true 时，任何类型的值为null对象时，均向以NULL值写入到数据库。
     * 当 this.defaultNull = false 时，
     *      1. String 类型的值，按 "" 空字符串写入到数据库 或 拼接成CQL语句
     *      2. 其它类型的值，以NULL值写入到数据库。
     * 
     * 默认为：false。
     * 
     * @param defaultNull
     */
    public void setDefaultNull(boolean defaultNull)
    {
        this.defaultNull = defaultNull;
    }
    
    
    
    /**
     * 获取：数据库连接信息
     */
    public DataSourceCQL getDataSourceCQL()
    {
        return dsCQL;
    }


    
    /**
     * 设置：数据库连接信息
     * 
     * @param i_DataSourceCQL
     */
    public void setDataSourceCQL(DataSourceCQL i_DataSourceCQL)
    {
        this.dsCQL = i_DataSourceCQL;
    }



    @Override
    public String toString()
    {
        return this.cqlText;
    }
}





/**
 * 填充占位符的类
 *
 * @author      ZhengWei(HY)
 * @createDate  2016-08-09
 * @version     v1.0
 */
interface DBCQLFill
{
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换首个占位符
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-09
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    public String fillFirst(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType);
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符。
     * 
     * 替换公式：i_Info.replaceAll("#" + i_PlaceHolder , i_Value.replaceAll("'" ,"''"));
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-09
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    public String fillAll(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType);
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符（前后带单引号的替换）
     * 
     * 替换公式：i_Info.replaceAll("'#" + i_PlaceHolder + "'", i_Value.replaceAll("'" ,"''"));
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-08-10
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    public String fillAllMark(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType);
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换首个占位符
     * 
     * 只填充，不替换特殊字符。主要用于 “条件DBConditions” ，条件中的数值交由开发者来决定
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-10-11
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    public String onlyFillFirst(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType);
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符。
     * 
     * 替换公式：i_Info.replaceAll("#" + i_PlaceHolder , i_Value);
     * 
     * 只填充，不替换特殊字符。主要用于 “条件DBConditions” ，条件中的数值交由开发者来决定
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-10-11
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    public String onlyFillAll(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType);
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符（前后带单引号的替换）
     * 
     * 替换公式：i_Info.replaceAll("'#" + i_PlaceHolder + "'", i_Value.replaceAll("'" ,"''"));
     * 
     * 只填充，不替换特殊字符。主要用于 “条件DBConditions” ，条件中的数值交由开发者来决定
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-10-11
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    public String onlyFillAllMark(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType);
    
    
    
    /**
     * 将占位符替换成空字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-09
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @return
     */
    public String fillSpace(String i_Info ,String i_PlaceHolder);
    
}





/**
 * 将占位符替换成数值。
 * 
 * 采用：单例模式
 *
 * @author      ZhengWei(HY)
 * @createDate  2016-08-09
 * @version     v1.0
 */
class DBCQLFillDefault implements DBCQLFill ,Serializable
{
    private static final long serialVersionUID = -8568480897505758512L;
    
    private static DBCQLFill $MySelf;
    
    
    public synchronized static DBCQLFill getInstance()
    {
        if ( $MySelf == null )
        {
            $MySelf = new DBCQLFillDefault();
        }
        
        return $MySelf;
    }
    
    
    private DBCQLFillDefault()
    {
        
    }
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换首个占位符
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-09
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String fillFirst(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        try
        {
            return StringHelp.replaceFirst(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,i_Value);
        }
        catch (Exception exce)
        {
            return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,Matcher.quoteReplacement(i_Value));
        }
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换首个占位符
     * 
     * 只填充，不替换特殊字符。主要用于 “条件DBConditions” ，条件中的数值交由开发者来决定
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-10-11
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String onlyFillFirst(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        return fillFirst(i_Info ,i_PlaceHolder ,i_Value ,i_DBType);
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符
     * 
     * 替换公式：i_Info.replaceAll("#" + i_PlaceHolder , i_Value.replaceAll("'" ,"''"));
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-09
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String fillAll(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        try
        {
            return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,i_Value);
        }
        catch (Exception exce)
        {
            return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,Matcher.quoteReplacement(i_Value));
        }
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符。
     * 
     * 替换公式：i_Info.replaceAll("#" + i_PlaceHolder , i_Value);
     * 
     * 只填充，不替换特殊字符。主要用于 “条件DBConditions” ，条件中的数值交由开发者来决定
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-10-11
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String onlyFillAll(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        return fillAll(i_Info ,i_PlaceHolder ,i_Value ,i_DBType);
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符（前后带单引号的替换）
     * 
     * 替换公式：i_Info.replaceAll("'#" + i_PlaceHolder + "'", i_Value.replaceAll("'" ,"''"));
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-08-10
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String fillAllMark(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        try
        {
            return StringHelp.replaceAll(i_Info ,"'" + DBCQL.$Placeholder + i_PlaceHolder + "'" ,i_Value);
        }
        catch (Exception exce)
        {
            return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,Matcher.quoteReplacement(i_Value));
        }
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符（前后带单引号的替换）
     * 
     * 替换公式：i_Info.replaceAll("'#" + i_PlaceHolder + "'", i_Value.replaceAll("'" ,"''"));
     * 
     * 只填充，不替换特殊字符。主要用于 “条件DBConditions” ，条件中的数值交由开发者来决定
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-10-11
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String onlyFillAllMark(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        return fillAllMark(i_Info ,i_PlaceHolder ,i_Value ,i_DBType);
    }
    
    
    
    /**
     * 将占位符替换成空字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-09
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String fillSpace(String i_Info ,String i_PlaceHolder)
    {
        return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,"");
    }
    
}





/**
 * 将数值中的单引号替换成两个单引号。单引号是数据库的字符串两边的限定符。
 * 如果占位符对应的数值中也存在单引号，会造成生成的CQL语句无法正确执行。
 * 是否替换可通过 DBCQL.keyReplace 属性控制。
 * 
 * 采用：单例模式
 *
 * @author      ZhengWei(HY)
 * @createDate  2016-08-09
 * @version     v1.0
 */
class DBCQLFillKeyReplace implements DBCQLFill ,Serializable
{
    private static final long serialVersionUID = 3135504177775635847L;

    public  static final String    $FillReplace         = "'";
    
    public  static final String    $FillReplaceBy       = "\'";
    
    private static       DBCQLFill $MySelf;
    
    
    /** 表示个别不替换数据库关键字的占位符。前缀无须冒号 */
    private Set<String> notKeyReplace;
    
    
    
    public synchronized static DBCQLFill getInstance(Set<String> i_NotKeyReplace)
    {
        if ( !Help.isNull(i_NotKeyReplace) )
        {
            return new DBCQLFillKeyReplace(i_NotKeyReplace);
        }
        
        if ( $MySelf == null )
        {
            $MySelf = new DBCQLFillKeyReplace();
        }
        
        return $MySelf;
    }
    
    
    private DBCQLFillKeyReplace()
    {
        this(null);
    }
    
    
    private DBCQLFillKeyReplace(Set<String> i_NotKeyReplace)
    {
        this.notKeyReplace = i_NotKeyReplace;
    }
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换首个占位符
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-09
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String fillFirst(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        try
        {
            if ( (this.notKeyReplace == null || !this.notKeyReplace.contains(i_PlaceHolder)) && this.isAllowReplace(i_Value) )
            {
                return StringHelp.replaceFirst(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,StringHelp.replaceAll(i_Value ,$FillReplace ,$FillReplaceBy));
            }
            else
            {
                return StringHelp.replaceFirst(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,i_Value);
            }
        }
        catch (Exception exce)
        {
            if ( (this.notKeyReplace == null || !this.notKeyReplace.contains(i_PlaceHolder)) && this.isAllowReplace(i_Value) )
            {
                return StringHelp.replaceFirst(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,Matcher.quoteReplacement(StringHelp.replaceAll(i_Value ,$FillReplace ,$FillReplaceBy)));
            }
            else
            {
                return StringHelp.replaceFirst(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,Matcher.quoteReplacement(i_Value));
            }
        }
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换首个占位符
     * 
     * 只填充，不替换特殊字符。主要用于 “条件DBConditions” ，条件中的数值交由开发者来决定
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-10-11
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String onlyFillFirst(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        try
        {
            return StringHelp.replaceFirst(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,i_Value);
        }
        catch (Exception exce)
        {
            return StringHelp.replaceFirst(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,Matcher.quoteReplacement(i_Value));
        }
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符
     * 
     * 替换公式：i_Info.replaceAll("#" + i_PlaceHolder , i_Value.replaceAll("'" ,"''"));
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-09
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String fillAll(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        try
        {
            if ( (this.notKeyReplace == null || !this.notKeyReplace.contains(i_PlaceHolder)) && this.isAllowReplace(i_Value) )
            {
                return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,StringHelp.replaceAll(i_Value ,$FillReplace ,$FillReplaceBy));
            }
            else
            {
                return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,i_Value);
            }
        }
        catch (Exception exce)
        {
            if ( (this.notKeyReplace == null || !this.notKeyReplace.contains(i_PlaceHolder)) && this.isAllowReplace(i_Value) )
            {
                return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,Matcher.quoteReplacement(StringHelp.replaceAll(i_Value ,$FillReplace ,$FillReplaceBy)));
            }
            else
            {
                return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,Matcher.quoteReplacement(i_Value));
            }
        }
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符。
     * 
     * 替换公式：i_Info.replaceAll("#" + i_PlaceHolder , i_Value);
     * 
     * 只填充，不替换特殊字符。主要用于 “条件DBConditions” ，条件中的数值交由开发者来决定
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-10-11
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String onlyFillAll(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        try
        {
            return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,i_Value);
        }
        catch (Exception exce)
        {
            return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,Matcher.quoteReplacement(i_Value));
        }
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符（前后带单引号的替换）
     * 
     * 替换公式：i_Info.replaceAll("'#" + i_PlaceHolder + "'", i_Value.replaceAll("'" ,"''"));
     * 
     * @author      ZhengWei(HY)
     * @createDate  2018-08-10
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String fillAllMark(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        String v_PH = "'" + DBCQL.$Placeholder + i_PlaceHolder + "'";
        
        try
        {
            if ( (this.notKeyReplace == null || !this.notKeyReplace.contains(i_PlaceHolder)) && this.isAllowReplace(i_Value) )
            {
                return StringHelp.replaceAll(i_Info ,v_PH ,StringHelp.replaceAll(i_Value ,$FillReplace ,$FillReplaceBy));
            }
            else
            {
                return StringHelp.replaceAll(i_Info ,v_PH ,i_Value);
            }
        }
        catch (Exception exce)
        {
            if ( (this.notKeyReplace == null || !this.notKeyReplace.contains(i_PlaceHolder)) && this.isAllowReplace(i_Value) )
            {
                return StringHelp.replaceAll(i_Info ,v_PH ,Matcher.quoteReplacement(StringHelp.replaceAll(i_Value ,$FillReplace ,$FillReplaceBy)));
            }
            else
            {
                return StringHelp.replaceAll(i_Info ,v_PH ,Matcher.quoteReplacement(i_Value));
            }
        }
    }
    
    
    
    /**
     * 将数值(i_Value)中的单引号替换成两个单引号后，再替换所有相同的占位符（前后带单引号的替换）
     * 
     * 替换公式：i_Info.replaceAll("'#" + i_PlaceHolder + "'", i_Value.replaceAll("'" ,"''"));
     * 
     * 只填充，不替换特殊字符。主要用于 “条件DBConditions” ，条件中的数值交由开发者来决定
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-10-11
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @param i_Value
     * @param i_DBType       数据库类型。见DataSourceGroup.$DBType_ 前缀的系列常量
     * @return
     */
    @Override
    public String onlyFillAllMark(String i_Info ,String i_PlaceHolder ,String i_Value ,String i_DBType)
    {
        String v_PH = "'" + DBCQL.$Placeholder + i_PlaceHolder + "'";
        
        try
        {
            return StringHelp.replaceAll(i_Info ,v_PH ,i_Value);
        }
        catch (Exception exce)
        {
            return StringHelp.replaceAll(i_Info ,v_PH ,Matcher.quoteReplacement(i_Value));
        }
    }
    
    
    
    /**
     * 将占位符替换成空字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-09
     * @version     v1.0
     *
     * @param i_Info
     * @param i_PlaceHolder
     * @return
     */
    @Override
    public String fillSpace(String i_Info ,String i_PlaceHolder)
    {
        return StringHelp.replaceAll(i_Info ,DBCQL.$Placeholder + i_PlaceHolder ,"");
    }
    
    
    
    /**
     * 是否允许替换字符串。防止如：'A' ,'B' ,'C' ... ,'Z'  这样格式的字符串被替换
     * 
     * 一般用于由外界动态生成的在 IN 语法中，如 IN ('A' ,'B' ,'C' ... ,'Z')，此时这里的单引号就不应被替换。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-08-23
     * @version     v1.0
     *
     * @param i_Value
     * @return
     */
    private boolean isAllowReplace(String i_Value)
    {
        int v_ACount = StringHelp.getCount(i_Value ,"'");
        
        // 当单引号成对出现时
        if ( v_ACount % 2 == 0 )
        {
            boolean v_StartW = i_Value.trim().startsWith("'");
            boolean v_EndW   = i_Value.trim().endsWith("'");
            
            if ( v_StartW && v_EndW )
            {
                int v_BCount = StringHelp.getCount(i_Value ,",");
                if ( v_ACount / 2 == v_BCount + 1 )
                {
                    // 当单引号成对的个数 = 分号的个数时，不允许作替换动作
                    return false;
                }
            }
            else if ( !v_StartW && !v_EndW )
            {
                int v_BCount = StringHelp.getCount(i_Value ,",");
                if ( v_ACount / 2 == v_BCount )
                {
                    // 当单引号成对的个数 = 分号的个数时，不允许作替换动作
                    return false;
                }
            }
        }
        
        return true;
    }
    
}
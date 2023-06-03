package org.hy.common.xcql;





/**
 * 解释 cfill 字符串后生成的方法的参数信息
 * 
 * 这样只须解释一次，在后面的行级填充动作时，可快速填充，而不用每次都解释 cfill 字符串。
 * 
 * @author      ZhengWei(HY)
 * @version     v1.0
 * @createDate  2023-06-02
 */
public final class XCQLMethodParam_CFill
{
    /** 列级填充时的常量之一：填充列级字段值 */
    public final static int    $CFILL_COL_VALUE       = 1;
    
    /** 列级填充时的常量之一：填充列级字段名称 */
    public final static int    $CFILL_COL_NAME        = 3;
    
    
    private final static XCQLMethodParam $CFILL_PARAM_COL_VALUE = new XCQLMethodParam_CFill_ColValue();
    
    private final static XCQLMethodParam $CFILL_PARAM_COL_NAME  = new XCQLMethodParam_CFill_ColName();
    
    
    
    /**
     * 获取参数信息的实例
     * 
     * @param i_ParamType
     * @return
     * @throws InstantiationException
     */
    public static XCQLMethodParam getInstance(int i_ParamType)
    {
        if ( i_ParamType == $CFILL_COL_VALUE )
        {
            return $CFILL_PARAM_COL_VALUE;
        }
        else if ( i_ParamType == $CFILL_COL_NAME )
        {
            return $CFILL_PARAM_COL_NAME;
        }
        else
        {
            throw new RuntimeException("Param type is error.");
        }
    }
    
    
    
    /**
     * 不允许被构造
     */
    private XCQLMethodParam_CFill()
    {
        
    }
    
}





/**
 * 字段数值为入参参数的情况。
 * 
 * 解释 cfill 字符串后生成的方法的参数信息
 * 
 * 这样只须解释一次，在后面的行级填充动作时，可快速填充，而不用每次都解释 cfill 字符串。
 * 
 * 独立出本类是为：减少填充数据时 if 语句的判断，改用预先解释好，填充数据时直接调用相关实例化的类，来提高性能
 * 
 * @author      ZhengWei(HY)
 * @version     v1.0
 * @createDate  2012-11-08
 */
class XCQLMethodParam_CFill_ColValue implements XCQLMethodParam
{
    
    /** 列级填充时的常理类型 */
    private int                paramType;
    
    
    
    public XCQLMethodParam_CFill_ColValue()
    {
        this.paramType = XCQLMethodParam_CFill.$CFILL_COL_VALUE;
    }
    
    
    
    /**
     * 执行后，得到将"子级"对象填充到"父级"对象中的父级填充方法的参数值
     * 如：行级对象填充到表级对象
     * 如：列级数值填充表行级对象
     * 
     * @param i_ColValue   填充对象。     可以行级对象 或 列级字段值
     * @param i_ColNo      填充对象的编号。当为行级对象时，为行号。  下标从 0 开始。
     *                                   当为列级字段值时，为空 null，Neo4j每个节点的属性可以不一致，没有固定的结构，
     *                                   也就无从列号之说了哈
     * @param i_ColName    填充对象的名称。当为行级对象时，可为空 null
     *                                  当为列级字段值时，为字段名称
     * @return             返回父级填充方法实际的入参数值
     */
    @Override
    public Object invoke(Object i_ColValue ,Long i_ColNo ,String i_ColName)
    {
        return i_ColValue;
    }
    
    
    
    public int getParamType()
    {
        return this.paramType;
    }
    
}





/**
 * 字段名称为入参参数的情况。
 * 
 * 解释 cfill 字符串后生成的方法的参数信息
 * 
 * 这样只须解释一次，在后面的行级填充动作时，可快速填充，而不用每次都解释 cfill 字符串。
 * 
 * 独立出本类是为：减少填充数据时 if 语句的判断，改用预先解释好，填充数据时直接调用相关实例化的类，来提高性能
 * 
 * @author      ZhengWei(HY)
 * @version     v1.0
 * @createDate  2012-11-08
 */
class XCQLMethodParam_CFill_ColName implements XCQLMethodParam
{
    
    /** 列级填充时的常理类型 */
    private int                paramType;
    
    
    
    public XCQLMethodParam_CFill_ColName()
    {
        this.paramType = XCQLMethodParam_CFill.$CFILL_COL_NAME;
    }
    
    
    
    /**
     * 执行后，得到将"子级"对象填充到"父级"对象中的父级填充方法的参数值
     * 如：行级对象填充到表级对象
     * 如：列级数值填充表行级对象
     * 
     * @param i_ColValue   填充对象。     可以行级对象 或 列级字段值
     * @param i_ColNo      填充对象的编号。当为行级对象时，为行号。  下标从 0 开始。
     *                                   当为列级字段值时，为空 null，Neo4j每个节点的属性可以不一致，没有固定的结构，
     *                                   也就无从列号之说了哈
     * @param i_ColName    填充对象的名称。当为行级对象时，可为空 null
     *                                  当为列级字段值时，为字段名称
     * @return             返回父级填充方法实际的入参数值
     */
    @Override
    public Object invoke(Object i_ColValue ,Long i_ColNo ,String i_ColName)
    {
        return i_ColName;
    }
    
    
    
    public int getParamType()
    {
        return this.paramType;
    }
    
}

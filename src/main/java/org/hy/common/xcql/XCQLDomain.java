package org.hy.common.xcql;





/**
 * 数据库连接的域。
 * 
 * 实现相同数据库结构下的，多个数据库间的分域功能。
 * 
 * 多个数据库间的相同CQL语句，不用重复写多次，只须通过"分域"动态改变数据库连接池组即可。
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-02
 * @version     v1.0
 */
public interface XCQLDomain
{
    
    /**
     * 可用会话Session中的用户信息来做判定标准
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-02
     * @version     v1.0
     *
     * @return
     */
    public DataSourceCQL getDataSourceCQL();
    
}

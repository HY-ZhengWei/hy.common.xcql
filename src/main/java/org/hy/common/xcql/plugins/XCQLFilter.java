package org.hy.common.xcql.plugins;

import java.io.IOException;
import java.util.ArrayList;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.hy.common.Busway;
import org.hy.common.ExpireCache;
import org.hy.common.Help;
import org.hy.common.StringHelp;
import org.hy.common.xcql.XCQLURL;





/**
 * 记录一次页面访问所对应执行的CQL信息
 * 
 * 过滤器的初始化参数：1. exclusions  排除哪些URL不被记录。多个正则表达式规则间用,逗号分隔
 *                2. cachesize   缓存大小。用于 $CQLBusway
 *                3. timeout     超时时长，单位：秒。用于 $Requests
 * 
 * 
    <!-- 记录一次页面访问所对应执行的CQL信息  ZhengWei(HY) Add 2023-06-02 -->
    <filter>
        <filter-name>XCQLFilter</filter-name>
        <filter-class>org.hy.common.xcql.plugins.XCQLFilter</filter-class>
        <init-param>
            <param-name>exclusions</param-name>
            <param-value>*.js,*.gif,*.jpg,*.png,*.css,*.ico,*.swf</param-value>
        </init-param>
        <init-param>
            <param-name>cachesize</param-name>
            <param-value>1000</param-value>
        </init-param>
        <init-param>
            <param-name>timeout</param-name>
            <param-value>60</param-value>
        </init-param>
    </filter>
    <filter-mapping>
        <filter-name>XCQLFilter</filter-name>
        <url-pattern>/*</url-pattern>
    </filter-mapping>
    
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-02
 * @version     v1.0
 */
public class XCQLFilter implements Filter
{
    
    private static final ExpireCache<Long ,XCQLURL> $Requests  = new ExpireCache<Long ,XCQLURL>();
    
    private static final Busway<XCQLURL>            $CQLBusway = new Busway<XCQLURL>(1000);
    
    
    
    /** 排除哪些URL不被记录 */
    private String [] filters;
    
    /** 超时时长。单位：秒 */
    private int       timeOut = 60;
    
    
    
    /**
     * 执行CQL的记录
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-07-13
     * @version     v1.0
     *
     * @param i_ThreadID
     * @param i_CQL
     */
    public synchronized static void logXCQL(Long i_ThreadID ,String i_CQL)
    {
        XCQLURL v_XCQLURL = $Requests.get(i_ThreadID);
        
        if ( v_XCQLURL != null )
        {
            if ( v_XCQLURL.getCqls() == null )
            {
                v_XCQLURL.setCqls(new ArrayList<String>());
                
                $CQLBusway.put(v_XCQLURL);
            }
            
            v_XCQLURL.getCqls().add(i_CQL);
        }
    }
    
    
    
    /**
     * 显示日志
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-07-13
     * @version     v1.0
     *
     * @return
     */
    public Busway<XCQLURL> logs()
    {
        return $CQLBusway;
    }
    
    
    
    /**
     * 获取日志
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-07-13
     * @version     v1.0
     *
     * @return
     */
    public Busway<XCQLURL> getLogs()
    {
        return $CQLBusway;
    }
    
    
    
    public void setLogs(Busway<XCQLURL> i_CQLBusway)
    {
        // Nothing.
    }
    
    
    
    /**
     * 清除日志
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-07-13
     * @version     v1.0
     *
     */
    public void clearLogs()
    {
        $CQLBusway.clear();
        $Requests .clear();
    }
    
    
    
    /**
     * 排除哪些URL不被记录
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-07-13
     * @version     v1.0
     *
     * @param i_URL
     * @return
     */
    private boolean isExclusions(String i_URL)
    {
        if ( Help.isNull(this.filters) )
        {
            return true;
        }
        else if ( StringHelp.getCount(i_URL ,this.filters) >= 1 )
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
    
    
    @Override
    public void init(FilterConfig i_FilterConfig) throws ServletException
    {
        // XJava.putObject("XCQLFilter" ,this);
        
        String v_Exclusions = i_FilterConfig.getInitParameter("exclusions");
        String v_Cachesize  = i_FilterConfig.getInitParameter("cachesize");
        String v_Timeout    = i_FilterConfig.getInitParameter("timeout");
        
        if ( !Help.isNull(v_Exclusions) )
        {
            this.filters = v_Exclusions.split(",");
            
            for (int i=0; i<this.filters.length; i++)
            {
                this.filters[i] = StringHelp.replaceAll(this.filters[i] ,new String[]{"*." ,"*"} ,new String[]{"[\\S]+"});
            }
        }
        else
        {
            this.filters = new String[0];
        }
        
        if ( Help.isNumber(v_Cachesize) )
        {
            $CQLBusway.setWayLength(Integer.parseInt(v_Cachesize));
        }
        
        if ( Help.isNumber(v_Timeout) )
        {
            this.timeOut = Integer.parseInt(v_Timeout);
        }
    }
    
    
    
    @Override
    public void doFilter(ServletRequest i_Request ,ServletResponse i_Response ,FilterChain i_Chain) throws IOException ,ServletException
    {
        HttpServletRequest v_Request = (HttpServletRequest)i_Request;
        String             v_URL     = v_Request.getRequestURL().toString();
        
        if ( !this.isExclusions(v_URL) )
        {
            XCQLURL v_XCQLURL = new XCQLURL();
            if ( !Help.isNull(v_Request.getQueryString()) )
            {
                v_XCQLURL.setUrl(v_URL + "?" + v_Request.getQueryString());
            }
            else
            {
                v_XCQLURL.setUrl(v_URL);
            }
            
            $Requests.put(Thread.currentThread().getId() ,v_XCQLURL ,this.timeOut);
        }
        
        i_Chain.doFilter(i_Request ,i_Response);
    }
    
    
    
    @Override
    public void destroy()
    {
        
    }
    
}

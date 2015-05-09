package org.apache.activemq.broker.jmx;

import javax.management.*;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A cache for Annotated MBean parameters, descriptions, etc.  In profiling,
 * reflection turned to to be a hot spot and called far too often.  For example,
 * with 20k queues, more than 8M reflected calls were performed which
 * increase overhead by more than 200x.  This relaxes that constrained by
 * computing a key for an operation, and then looking it up in a cache.  The
 * memory overhead for the cache at this point was only 200 items.  So a clear
 * space/time tradeoff.
 */
public class AnnotatedMBeanCache {

    // keep hit and miss information.  This could be exported via JMX in the
    // future but the primary reason is debug information on how well the cache
    // is performing.  Keeping stat on hits and misses did not impose any
    // significant overhead.
    private static AtomicLong hits = new AtomicLong( 0 );
    private static AtomicLong misses = new AtomicLong( 0 );

    private static Map<String,String> cache = new ConcurrentHashMap<String, String>();

    public void update(String key, String value) {

        cache.put( key, value );

        misses.getAndIncrement();
    }

    public String get(String key) {

        String value = cache.get( key );

        if ( value != null ) {
            hits.getAndIncrement();
        }

        return value;

    }

    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    public int size() {
        return cache.size();
    }

    public String computeCacheKey(AnnotatedMBean annotatedMBean, MBeanOperationInfo op, MBeanParameterInfo param, int paramNo) {

        StringBuilder result = new StringBuilder();

        result.append(computeCacheKey(annotatedMBean,op));
        result.append(".");
        result.append(param.getName());
        result.append(".");
        result.append(paramNo);

        return result.toString();

    }

    public String computeCacheKey(AnnotatedMBean annotatedMBean, MBeanAttributeInfo mBeanAttributeInfo) {
        StringBuilder result = new StringBuilder();
        result.append( mBeanAttributeInfo.getName() );
        return result.toString();
    }

    public String computeCacheKey( AnnotatedMBean annotatedMBean, MBeanOperationInfo mBeanOperationInfo) {

        StringBuilder result = new StringBuilder();
        result.append( annotatedMBean.getMBeanInterface().getName() );

        final MBeanParameterInfo[] params = mBeanOperationInfo.getSignature();
        for (int i = 0; i < params.length; i++) {
            String type = params[i].getType();
            result.append( "." );
            result.append( type );
        }

        return result.toString();

    }

}

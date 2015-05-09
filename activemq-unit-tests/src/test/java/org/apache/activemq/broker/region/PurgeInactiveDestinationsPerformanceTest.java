package org.apache.activemq.broker.region;

import com.google.common.base.Stopwatch;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.After;
import org.junit.Before;

import javax.jms.*;
import java.net.URI;

/**
 *
 */
public class PurgeInactiveDestinationsPerformanceTest extends TestCase {

    public static final int NR_QUEUES = 20000;

    public static final int NR_MESSAGES_PER_QUEUE = 1;

    public static final String BROKER_HOST = "127.0.0.1";

    public static final int BROKER_PORT = 16161;

    protected static String brokerURL = String.format( "tcp://%s:%s", BROKER_HOST, BROKER_PORT );

    protected BrokerService broker;

    // TODO:
    //
    // - test creating queues without JMX (25s)
    // - test creating queues with JMX (easily 20x slower and lots more GC issues)
    // - test with 1 queue and 20k messages to see if the createQueues time is the same.
    //          - definitely faster.. about 20x faster.
    // - test with leveldb to see what the overhead looks like.

    @Before
    public void setUp() throws Exception {

        System.out.printf( "Loading from %s\n", getClass().getClassLoader() );

        // ****  Turn on stdout logging in log4j
        DOMConfigurator.configure( getClass().getResource( "/log4j-stdout.xml" ) );

        broker = BrokerFactory.createBroker( new URI( "xbean:purge-inactive-destinations-performance.xml" ) );

        broker.start();
    }

    @After
    public void tearDown() throws Exception {

        if (broker != null)
            broker.stop();

    }

    public void test1() throws Exception {

        // create queue overhead may be the same as message overhead.
        createQueues();

        //purgeInactiveDestinations();

        System.out.printf( "hits: %s\n", AnnotatedMBean.annotatedMBeanCache.hits.get() );
        System.out.printf( "misses: %s\n", AnnotatedMBean.annotatedMBeanCache.misses.get() );

    }

    private void purgeInactiveDestinations() {
        RegionBroker regionBroker = (RegionBroker)broker.getRegionBroker();

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        System.out.printf( "=== STARTING QUEUE GC PURGE!\n" );

        regionBroker.purgeInactiveDestinations();

        System.out.printf( "=============\n" );
        System.out.printf( "Purged inactive destinations in : %s\n", stopwatch.stop() );
    }

    private void createQueues() throws Exception {

        System.out.printf( "Creating queues..." );

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory( brokerURL );
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        for (int i = 1; i <= NR_QUEUES; i++) {

            String queueName = "test-" + i;

            javax.jms.Queue dest = session.createQueue( queueName );

            MessageProducer producer = session.createProducer( dest );
            producer.setDeliveryMode( DeliveryMode.NON_PERSISTENT );
            MessageConsumer consumer = session.createConsumer( dest );

            for (int j = 1; j <= NR_MESSAGES_PER_QUEUE; j++) {

                javax.jms.Message message = session.createTextMessage( "xx" );
                producer.send( message );

                // now receive the message we just produced so the queue has an empty size.
                consumer.receive();

                if ( (j % 100) == 0 ) {
                    System.out.printf( "[%s]", j );
                }

            }

            producer.close();
            consumer.close();

            if ( (i % 100) == 0 ) {
                System.out.printf( "%s. ", i );
            }

        }

        System.out.printf( "\n" );
        System.out.printf( "Creating queues...done (%s)\n", stopwatch.stop() );

    }

}
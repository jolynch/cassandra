package org.apache.cassandra.repair.scheduler.conn;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.repair.scheduler.RepairUtil;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This is the class that all CassandraInteractions should inherit from. It attempts to implement the functionality
 * that shouldn't change between different versions of Cassandra such as JMX connection management, basic cluster
 * information, etc ... Methods from the {@link CassandraInteraction} interface that are not implemented here
 * which do change heavily should be implemented in version specific implementation classes
 * (e.g. {@link Cass4xInteraction}).
 *
 * Methods in the base should _only_ be those that are compatible with all active releases of Cassandra. In this case
 * this class should work with 3.0.x, 3.11.x and 4.x (this class does not work with 2.1, or older releases). If you
 * need 2.1 support just copy and paste this into a different class that implements CassandraInteraction and fix up any
 * compilation issues
 */
public abstract class CassandraInteractionBase implements CassandraInteraction, AutoCloseable
{
    protected final TaskSchedulerConfig config;

    protected static final Logger logger = LoggerFactory.getLogger(Cass4xInteraction.class);
    // Listener management
    protected final Set<NotificationListener> repairListeners = new HashSet<>();
    // Mbean Management
    protected final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    protected MBeanServerConnection mbeanServer;
    protected CompactionManagerMBean cmProxy;
    protected FailureDetectorMBean fdProxy;
    protected StorageServiceMBean ssProxy;
    protected JMXConnector jmxConnector = null;
    protected ObjectName ssMbeanName;
    protected JMXServiceURL jmxUrl;
    protected volatile Timer timer = new Timer();

    // Caches for very frequently called metadata
    protected boolean isConnected = false;
    protected boolean reconnectionScheduled = false;
    protected long lastConnectionLookup = 0;
    protected String cachedConnectionId = null;
    protected String cachedHostId = null;
    protected IPartitioner partitioner = null;

    /**
     * Constructor that does absolutely no connecting or talking to Cassandra at all. Any interaction must be
     * deferred to the first usage of the API methods themselves. This is important for testing and reliability.
     * Do not change this behavior.
     *
     * @param config The configuration to use when setting up connections.
     */
    public CassandraInteractionBase(TaskSchedulerConfig config)
    {
        this.config = config;
    }

    @Override
    public void addRepairNotificationListener(NotificationListener listener)
    {
        RepairUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        synchronized (repairListeners)
        {
            if (!repairListeners.contains(listener))
            {
                jmxConnector.addConnectionNotificationListener(listener, null, null);
                ssProxy.addNotificationListener(listener, null, null);
                repairListeners.add(listener);
            }
        }
    }

    @Override
    public void removeRepairNotificationListener(NotificationListener listener)
    {
        RepairUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        synchronized (repairListeners)
        {
            try
            {
                if (repairListeners.contains(listener))
                {
                    ssProxy.removeNotificationListener(listener);
                    jmxConnector.removeConnectionNotificationListener(listener);
                }
            }
            catch (ListenerNotFoundException ignored)
            {
            }
            finally
            {
                repairListeners.remove(listener);
            }
        }
    }

    @Override
    public Set<NotificationListener> getOutstandingRepairNotificationListeners()
    {
        return ImmutableSet.copyOf(repairListeners);
    }

    @Override
    public String getLocalHostId()
    {
        RepairUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        // This is a really frequently called method and it doesn't change
        if (cachedHostId == null || System.currentTimeMillis() > (lastConnectionLookup + config.getJmxCacheTTL()))
            cachedHostId = ssProxy.getLocalHostId();
        return cachedHostId;
    }

    @Override
    public String getLocalLoadString()
    {
        RepairUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        return ssProxy.getLoadString();
    }

    @Override
    public String getClusterName()
    {
        RepairUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        return ssProxy.getClusterName();
    }

    @Override
    public Range<Token> tokenRangeFromStrings(String leftToken, String rightToken)
    {
        Token left, right;
        left = partitioner.getTokenFactory().fromString(leftToken);
        right = partitioner.getTokenFactory().fromString(rightToken);
        return new Range<>(left, right);
    }

    @Override
    public List<Range<Token>> getTokenRanges(String keyspace, boolean primaryOnly)
    {
        // Try to use the same definition as Cassandra for "primary token range"
        // namely any token range where this node's endpoint is first in the list
        String localEndpoint = this.getLocalEndpoint();
        Map<Range<Token>, List<String>> myTokenRanges = this.getRangeToEndpointMap(keyspace);

        Predicate<Range<Token>> rangeFilter;
        if (primaryOnly)
            rangeFilter = tr -> myTokenRanges.get(tr).get(0).equals(localEndpoint);
        else
            rangeFilter = tr -> myTokenRanges.get(tr).contains(localEndpoint);

        return myTokenRanges.keySet().stream()
                            .filter(rangeFilter)
                            // Try to yield primary ranges which are owned by the same replicas in order
                            // This way we will not spray repair over the whole cluster at once in the
                            // case of 256 vnodes, also we can potentially join contiguous ranges
                            // if Cassandra would stop complaining about imprecise repair
                            .collect(Collectors.groupingBy(myTokenRanges::get))
                            .entrySet().stream()
                            // Keep the ranges sorted within the replica sets, so for single token
                            // deployments we keep the primary ranges as contiguous as possible
                            .flatMap(e -> e.getValue().stream().sorted())
                            .collect(Collectors.toList());
    }

    public void triggerCompaction(String keyspace, String... columnFamilies)
    {
        RepairUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        logger.info(String.format("Triggering compaction for table %s.[%s]", keyspace, Arrays.asList(columnFamilies)));
        try
        {
            ssProxy.forceKeyspaceCompaction(false, keyspace, columnFamilies);
        }
        catch (IOException | ExecutionException | InterruptedException e)
        {
            logger.error(String.format("Failed to compact %s.[%s]", keyspace, Arrays.asList(columnFamilies)), e);
        }
    }

    public void triggerCleanup(int jobs, String keyspace, String... columnFamilies)
    {
        RepairUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        logger.info(String.format("Triggering cleanup for table %s.[%s]", keyspace, Arrays.asList(columnFamilies)));
        try
        {
            ssProxy.forceKeyspaceCleanup(jobs, keyspace, columnFamilies);
        }
        catch (IOException | ExecutionException | InterruptedException e)
        {
            logger.error(String.format("Failed to cleanup %s.[%s]", keyspace, Arrays.asList(columnFamilies)), e);
        }
    }

    public void triggerFlush(String keyspace, String... columnFamilies)
    {
        RepairUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        logger.info(String.format("Triggering flush for table %s.%s", keyspace, Arrays.asList(columnFamilies)));
        try
        {
            ssProxy.forceKeyspaceFlush(keyspace, columnFamilies);
        }
        catch (IOException | ExecutionException | InterruptedException e)
        {
            logger.error(String.format("Failed to flush %s.%s", keyspace, Arrays.asList(columnFamilies)), e);
        }
    }

    /** JMX Connection management
     */

    public Optional<String> getJMXConnectionId(boolean useCache)
    {
        // We call this method a _lot_ to check the health of the connection, and if we don't have
        // some sort of cache the Repair Scheduler can't make the JMX calls fast enough
        if (jmxConnector != null && (!useCache ||
                                     (System.currentTimeMillis() > (lastConnectionLookup + config.getJmxCacheTTL()))))
        {
            try
            {
                cachedConnectionId = jmxConnector.getConnectionId();
                lastConnectionLookup = System.currentTimeMillis();
            }
            catch (IOException | NullPointerException ignored)
            {
                logger.error("Error connecting", ignored);
            }
        }
        String connectionId = cachedConnectionId;
        if (connectionId != null)
            return Optional.of(cachedConnectionId);
        return Optional.empty();
    }

    public boolean isConnectionAlive(boolean useCache)
    {
        return getJMXConnectionId(useCache).filter(s -> s.length() > 0).isPresent();
    }

    public void connectAsync()
    {
        scheduleReconnector();
    }

    public boolean connectSync()
    {
        try
        {
            doConnect();
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
        finally
        {
            connectAsync();
        }
    }

    // Synchronized because of the timer manipulation
    protected synchronized void scheduleReconnector()
    {
        if (reconnectionScheduled)
            return;

        int delay = 100;   // delay for 100ms before trying to connect
        int period = config.getJmxConnectionMonitorPeriodInMs();  // repeat every 60 sec.
        timer.cancel();
        timer.purge();
        timer = new Timer();
        logger.info("Scheduling JMX connection monitor with initial delay of {} ms and interval of {} ms.", delay, period);
        timer.scheduleAtFixedRate(new TimerTask()
        {
            public void run()
            {
                tryGetConnection(false);
            }
        }, delay, period);
        reconnectionScheduled = true;
    }

    protected boolean tryGetConnection(boolean useCache)
    {
        boolean connectionAlive = isConnectionAlive(useCache);

        connectionAlive = connectionAlive || connectSync();
        return isConnected && connectionAlive;
        //return connectionAlive;
    }

    protected boolean tryGetConnection()
    {
        return tryGetConnection(true);
    }

    // Synchronized because mixing beans is a bad plan
    protected synchronized void doConnect()
    {
        ObjectName cmMbeanName, fdMbeanName;
        try
        {
            isConnected = false;
            jmxUrl = new JMXServiceURL(String.format(JMX_URL, config.getLocalJmxAddress(), config.getLocalJmxPort()));
            ssMbeanName = new ObjectName("org.apache.cassandra.db:type=StorageService");
            cmMbeanName = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
            fdMbeanName = new ObjectName(FailureDetector.MBEAN_NAME);
        }
        catch (MalformedURLException | MalformedObjectNameException e)
        {
            logger.error(String.format("Failed to prepare JMX connection to %s", jmxUrl), e);
            return;
        }
        try
        {
            jmxConnector = JMXConnectorFactory.connect(jmxUrl);
            mbeanServer = jmxConnector.getMBeanServerConnection();
            repairListeners.clear();
            ssProxy = JMX.newMBeanProxy(mbeanServer, ssMbeanName, StorageServiceMBean.class);
            cmProxy = JMX.newMBeanProxy(mbeanServer, cmMbeanName, CompactionManagerMBean.class);
            fdProxy = JMX.newMBeanProxy(mbeanServer, fdMbeanName, FailureDetectorMBean.class);
            setPartitioner(FBUtilities.newPartitioner(ssProxy.getPartitionerName()));

            isConnected = true;
            logger.info(String.format("JMX connection to %s properly connected", jmxUrl));
        }
        catch (Exception e)
        {
            String msg = String.format("Failed to establish JMX connection to %s", jmxUrl);
            logger.error(msg, e);
        }
    }

    /**
     * Cleanly shut down by un-registering the listeners and closing the JMX connection.
     */
    public void close()
    {
        logger.debug(String.format("Closing JMX connection to %s", jmxUrl));
        getOutstandingRepairNotificationListeners().forEach(this::removeRepairNotificationListener);
        try
        {
            jmxConnector.close();
        }
        catch (IOException e)
        {
            logger.warn("Failed to close JMX connection.", e);
        }
    }

    @VisibleForTesting
    public void setPartitioner(IPartitioner partitioner)
    {
        if(this.partitioner == null)
        {
            this.partitioner = partitioner;
        }
    }
}

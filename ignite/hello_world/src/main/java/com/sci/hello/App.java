package com.sci.hello;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import java.io.Serializable;
import java.util.Arrays;

public class App {

    public static void main(String[] args) throws IgniteException {
        try {
          TcpDiscoverySpi spi = new TcpDiscoverySpi();

          TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

          // Set initial IP addresses.
          // Note that you can optionally specify a port or a port range.
          ipFinder.setAddresses(Arrays.asList("127.0.0.1", "127.0.0.1:47500..47509"));

          spi.setIpFinder(ipFinder);

          IgniteConfiguration cfg = new IgniteConfiguration();

          // Override default discovery SPI.
          cfg.setDiscoverySpi(spi);

          cfg.setPeerClassLoadingEnabled(true);

          // Start Ignite node.
          Ignite ignite = Ignition.start(cfg);
          // Ignite ignite = Ignition.start("examples/config/example-default.xml");
          // Put values in cache.

          final IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache").withKeepBinary();
          cache.put(1, "Hello");
          cache.put(2, "World!");
          // Get values from cache
          // Broadcast 'Hello World' on all the nodes in the cluster.
          ignite.compute().broadcast(
                                     new IgniteRunnable() {
                                         @Override public void run() {
                                             System.out.println(cache.get(1) + " " + cache.get(2));
                                        }
                                     });
        } catch(Exception e) {
            System.out.println("Exception: " + e);
        }
    }
}

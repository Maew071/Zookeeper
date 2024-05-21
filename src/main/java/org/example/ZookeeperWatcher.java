package org.example;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ZookeeperWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
        System.out.println("Received event: " + event);
    }
}


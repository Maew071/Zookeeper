package org.example;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class ZookeeperHierarchyDownloader {

    private static final String ZOOKEEPER_HOST = "localhost";
    private static final int ZOOKEEPER_PORT = 2181;

    public static void main(String[] args) {
        String rootPath = "/"; // Корневой путь в Zookeeper
        String localPath = "zookeeper_hierarchy"; // Локальный путь для сохранения иерархии

        try {
            ZookeeperConnectionWatcher connectionWatcher = new ZookeeperConnectionWatcher();
            ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_HOST + ":" + ZOOKEEPER_PORT, 3000, connectionWatcher);

            // Ensure the root directory exists
            File rootDir = new File(localPath);
            if (!rootDir.exists() && !rootDir.mkdirs()) {
                throw new IOException("Failed to create root directory: " + localPath);
            }

            ZookeeperHierarchyDownloader downloader = new ZookeeperHierarchyDownloader();
            downloader.downloadHierarchy(zooKeeper, rootPath, localPath);
            zooKeeper.close();
            System.out.println("Иерархия Zookeeper успешно скачана на диск.");
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    private void downloadHierarchy(ZooKeeper zooKeeper, String nodePath, String localPath)
            throws KeeperException, InterruptedException, IOException {
        List<String> children = zooKeeper.getChildren(nodePath, false);
        if (children.isEmpty()) {
            // Если у узла нет дочерних, сохраняем его содержимое как файл
            byte[] data = zooKeeper.getData(nodePath, false, null);
            saveFile(localPath + (nodePath.equals("/") ? "/root" : nodePath), data);
        } else {
            // Если у узла есть дочерние узлы, создаем директорию и продолжаем рекурсивно скачивать их
            File directory = new File(localPath + nodePath);
            if (!directory.exists() && !directory.mkdirs()) {
                throw new IOException("Failed to create directory: " + directory.getAbsolutePath());
            }
            for (String child : children) {
                downloadHierarchy(zooKeeper, nodePath + "/" + child, localPath);
            }
        }
    }

    private void saveFile(String path, byte[] data) throws IOException {
        File file = new File(path);
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(data);
        }
        System.out.println("Saved file: " + file.getAbsolutePath());
    }
}


package com.github.wxk6b1203.cli;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

@Slf4j
@CommandLine.Command(name = "server", mixinStandardHelpOptions = true, versionProvider = Version.class, description = "Lucene S3 Server")
public class Server implements Callable<Integer> {
    @CommandLine.Option(names = {"-c", "--conf"}, description = "Path to configuration file")
    private String configFile;

    @Override
    public Integer call() {
        log.info("Starting Lucene S3 Server with config file: {}", configFile);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // TODO: Add shutdown logic here
            log.info("Shutting down Lucene S3 Server...");
            countDownLatch.countDown();
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 1;
        }
        return CommandLine.ExitCode.OK;
    }
}

package com.github.wxk6b1203.cli;

import lombok.Data;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@Data
@CommandLine.Command(name = "server", mixinStandardHelpOptions = true, versionProvider = Version.class, description = "Lucene S3 Server")
public class Server implements Callable<Integer> {
    @CommandLine.Option(names = {"-c", "--conf"}, description = "Path to configuration file")
    private String configFile;

    @Override
    public Integer call() {
        System.out.println("Running Lucene S3 CLI with config: " + configFile);
        return CommandLine.ExitCode.OK;
    }
}

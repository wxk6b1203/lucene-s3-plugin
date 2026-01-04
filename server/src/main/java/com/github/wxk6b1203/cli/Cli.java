package com.github.wxk6b1203.cli;

import picocli.CommandLine;

@CommandLine.Command(name = "LuceneS3",
        subcommands = {Server.class},
        mixinStandardHelpOptions = true,
        versionProvider = Version.class,
        description = "Lucene S3 CLI Tool")
public class Cli {
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested;
}

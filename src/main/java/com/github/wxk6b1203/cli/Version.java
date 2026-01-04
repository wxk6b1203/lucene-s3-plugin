package com.github.wxk6b1203.cli;

import picocli.CommandLine;

public class Version implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() {
        // Prefer manifest value set by Gradle; fallback to placeholder when unavailable (e.g., IDE run)
        String implementationVersion = Version.class.getPackage().getImplementationVersion();
        if (implementationVersion == null || implementationVersion.isBlank()) {
            implementationVersion = "unknown";
        }
        return new String[]{implementationVersion};
    }
}

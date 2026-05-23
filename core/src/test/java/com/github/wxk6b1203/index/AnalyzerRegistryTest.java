package com.github.wxk6b1203.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class AnalyzerRegistryTest {
    @TempDir
    Path tempDir;

    @Test
    void loadsAnalyzerFromPluginDirectory() throws Exception {
        Path pluginClasses = tempDir.resolve("plugins").resolve("classes");
        Path sourceRoot = tempDir.resolve("src").resolve("plugin");
        Files.createDirectories(sourceRoot);
        Files.createDirectories(pluginClasses);
        Path source = sourceRoot.resolve("PluginKeywordAnalyzer.java");
        Files.writeString(source, """
                package plugin;

                public class PluginKeywordAnalyzer extends org.apache.lucene.analysis.Analyzer {
                    @Override
                    protected TokenStreamComponents createComponents(String fieldName) {
                        return new TokenStreamComponents(new org.apache.lucene.analysis.core.KeywordTokenizer());
                    }
                }
                """);
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "JDK compiler is required for analyzer plugin loading test");
        int result = compiler.run(
                null,
                null,
                null,
                "-classpath",
                System.getProperty("java.class.path"),
                "-d",
                pluginClasses.toString(),
                source.toString()
        );
        assertEquals(0, result);

        try (AnalyzerRegistry registry = new AnalyzerRegistry(tempDir.resolve("plugins"))) {
            Analyzer analyzer = registry.analyzer("class:plugin.PluginKeywordAnalyzer");
            assertEquals(List.of("Lucene Plugin"), tokens(analyzer, "title", "Lucene Plugin"));
        }
    }

    private List<String> tokens(Analyzer analyzer, String field, String value) throws Exception {
        List<String> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(field, value)) {
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                tokens.add(term.toString());
            }
            stream.end();
        }
        return tokens;
    }
}

package com.github.wxk6b1203.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

final class AnalyzerRegistry implements AutoCloseable {
    static final String DEFAULT_ANALYZER = "standard";

    private final Map<String, Analyzer> analyzers = new ConcurrentHashMap<>();
    private final URLClassLoader pluginClassLoader;
    private final ClassLoader analyzerClassLoader;

    AnalyzerRegistry() {
        this(null);
    }

    AnalyzerRegistry(Path pluginPath) {
        this.pluginClassLoader = pluginClassLoader(pluginPath);
        this.analyzerClassLoader = pluginClassLoader == null
                ? Thread.currentThread().getContextClassLoader()
                : pluginClassLoader;
    }

    Analyzer analyzer(String name) {
        String key = normalize(name);
        return analyzers.computeIfAbsent(key, this::createAnalyzer);
    }

    private Analyzer createAnalyzer(String name) {
        return switch (name) {
            case "standard" -> new StandardAnalyzer();
            case "keyword" -> new KeywordAnalyzer();
            case "whitespace" -> new WhitespaceAnalyzer();
            case "simple" -> new SimpleAnalyzer();
            case "stop" -> new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
            case "english" -> new EnglishAnalyzer();
            case "ik", "ik_max_word" -> classAnalyzer("org.wltea.analyzer.lucene.IKAnalyzer", false);
            case "ik_smart" -> classAnalyzer("org.wltea.analyzer.lucene.IKAnalyzer", true);
            case "pinyin" -> firstAvailableClassAnalyzer(
                    "org.elasticsearch.plugin.analysis.pinyin.PinyinAnalyzer",
                    "org.elasticsearch.index.analysis.PinyinAnalyzer",
                    "com.github.stuxuhai.jpinyin.lucene.PinyinAnalyzer"
            );
            default -> classAnalyzer(className(name), null);
        };
    }

    private Analyzer firstAvailableClassAnalyzer(String... classNames) {
        RuntimeException failure = null;
        for (String className : classNames) {
            try {
                return classAnalyzer(className, null);
            } catch (RuntimeException e) {
                failure = e;
            }
        }
        throw failure == null ? new IllegalArgumentException("pinyin analyzer class is not available") : failure;
    }

    private Analyzer classAnalyzer(String className, Boolean booleanConstructorArg) {
        try {
            Class<?> type = Class.forName(className, true, analyzerClassLoader);
            if (!Analyzer.class.isAssignableFrom(type)) {
                throw new IllegalArgumentException("analyzer class must extend Lucene Analyzer: " + className);
            }
            if (booleanConstructorArg != null) {
                try {
                    Constructor<?> constructor = type.getDeclaredConstructor(boolean.class);
                    constructor.setAccessible(true);
                    return (Analyzer) constructor.newInstance(booleanConstructorArg);
                } catch (NoSuchMethodException ignored) {
                    // fall through to no-arg constructor
                }
            }
            Constructor<?> constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
            return (Analyzer) constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("failed to load analyzer class: " + className, e);
        }
    }

    private String className(String name) {
        if (name.startsWith("class:")) {
            return name.substring("class:".length()).trim();
        }
        if (!name.contains(".")) {
            throw new IllegalArgumentException("unsupported analyzer: " + name
                    + ". Use a built-in analyzer name or class:<fully-qualified-class-name>.");
        }
        return name;
    }

    private String normalize(String name) {
        if (name == null || name.isBlank()) {
            return DEFAULT_ANALYZER;
        }
        String trimmed = name.trim();
        if (trimmed.startsWith("class:")) {
            return "class:" + trimmed.substring("class:".length()).trim();
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }

    private URLClassLoader pluginClassLoader(Path pluginPath) {
        if (pluginPath == null) {
            return null;
        }
        Path normalized = pluginPath.toAbsolutePath().normalize();
        if (!Files.exists(normalized)) {
            throw new IllegalArgumentException("analyzer plugin path does not exist: " + normalized);
        }
        try {
            List<URL> urls;
            if (Files.isDirectory(normalized)) {
                urls = new ArrayList<>();
                urls.add(url(normalized));
                try (Stream<Path> stream = Files.list(normalized)) {
                    urls.addAll(stream
                            .filter(path -> Files.isDirectory(path) || path.getFileName().toString().endsWith(".jar"))
                            .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                            .map(this::url)
                            .toList());
                }
            } else {
                urls = List.of(url(normalized));
            }
            return new URLClassLoader(urls.toArray(URL[]::new), Analyzer.class.getClassLoader());
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to load analyzer plugins from: " + normalized, e);
        }
    }

    private URL url(Path path) {
        try {
            return path.toUri().toURL();
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid analyzer plugin path: " + path, e);
        }
    }

    @Override
    public void close() {
        analyzers.values().forEach(Analyzer::close);
        analyzers.clear();
        if (pluginClassLoader != null) {
            try {
                pluginClassLoader.close();
            } catch (IOException ignored) {
            }
        }
    }
}

package com.github.wxk6b1203.processor.metadata;


import com.github.wxk6b1203.processor.metadata.annotation.Provider;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.Writer;
import java.util.*;

@SupportedAnnotationTypes("com.github.wxk6b1203.processor.metadata.annotation.Provider")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class ProviderEnumProcessor extends AbstractProcessor {

    private boolean generated = false;

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (generated) return true;
        if (roundEnv.processingOver()) return true;

        List<Entry> entries = new ArrayList<>();
        Set<String> keys = new HashSet<>();

        for (Element e : roundEnv.getElementsAnnotatedWith(Provider.class)) {
            if (e.getKind() != ElementKind.CLASS) continue;

            TypeElement te = (TypeElement) e;
            Provider an = te.getAnnotation(Provider.class);
            if (an == null) continue;

            String key = an.value();
            if (!keys.add(key)) {
                processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "Duplicate provider key: " + key,
                        e
                );
                return true;
            }

            String clazz = processingEnv.getElementUtils().getBinaryName(te).toString();
            entries.add(new Entry(key, clazz));
        }

        if (entries.isEmpty()) return true;

        entries.sort(Comparator.comparing(a -> a.key));

        try {
            generateEnum(entries);
            generated = true;
        } catch (Exception ex) {
            processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to generate MetadataProviders: " + ex.getClass().getName() + ": " + ex.getMessage()
            );
        }

        return true;
    }

    private void generateEnum(List<Entry> entries) throws Exception {
        String pkg = "com.github.wxk6b1203.metadata.provider";
        String simpleName = "MetadataProviders";

        Filer filer = processingEnv.getFiler();
        JavaFileObject file = filer.createSourceFile(pkg + "." + simpleName);

        try (Writer w = file.openWriter()) {
            w.write("package " + pkg + ";\n\n");
            w.write("public enum " + simpleName + " {\n");

            for (int i = 0; i < entries.size(); i++) {
                Entry e = entries.get(i);
                String enumName = toEnumName(e.key);

                w.write("    " + enumName + "(\"" + escapeJava(e.key) + "\", \"" + escapeJava(e.clazz) + "\")");
                w.write(i == entries.size() - 1 ? ";\n\n" : ",\n");
            }

            w.write("    public final String key;\n");
            w.write("    public final String implClassName;\n\n");
            w.write("    " + simpleName + "(String key, String implClassName) {\n");
            w.write("        this.key = key;\n");
            w.write("        this.implClassName = implClassName;\n");
            w.write("    }\n");
            w.write("}\n");
        }
    }

    private static String toEnumName(String key) {
        String s = key.trim().toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]+", "_");
        if (s.isEmpty() || Character.isDigit(s.charAt(0))) s = "_" + s;
        return s;
    }

    private static String escapeJava(String s) {
        return s.replace("\\\\", "\\\\\\\\").replace("\"", "\\\\\"");
    }

    private record Entry(String key, String clazz) {
    }
}

package com.github.wxk6b1203.util;

import org.junit.jupiter.api.Test;

public class YamlTest {
    @Test
    public void testYaml() {
        String yaml = """
                a: 1
                b:
                  c: 2
                  d:
                    e: 3
                f: 4
                """;
        var flattened = YamlUtil.readValueAsFlattenedMap(yaml);
        assert flattened.size() == 4;
        assert flattened.get("a").equals(1);
        assert flattened.get("b.c").equals(2);
        assert flattened.get("b.d.e").equals(3);
        assert flattened.get("f").equals(4);
    }
}

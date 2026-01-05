package com.github.wxk6b1203.util;

import org.junit.jupiter.api.Test;

public class JsonTest {
    @Test
    public void testJson() {
        String json = "{\"a\":1,\"b\":{\"c\":2,\"d\":{\"e\":3}},\"f\":4}";
        var flattened = JsonUtil.readValueAsFlattenedMap(json);
        assert flattened.size() == 4;
        assert flattened.get("a").equals(1);
        assert flattened.get("b.c").equals(2);
        assert flattened.get("b.d.e").equals(3);
        assert flattened.get("f").equals(4);
    }

    @Test
    public void testJsonWithArray() {
        String json = "{\"a\":1,\"b\":{\"c\":[1,2,{\"d\":3}]},\"e\":4}";
        var flattened = JsonUtil.readValueAsFlattenedMap(json);
        assert flattened.size() == 5;
        assert flattened.get("a").equals(1);
        assert flattened.get("b.c[0]").equals(1);
        assert flattened.get("b.c[1]").equals(2);
        assert flattened.get("b.c[2].d").equals(3);
        assert flattened.get("e").equals(4);
    }
}

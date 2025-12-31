package com.github.wxk6b1203;

import org.apache.lucene.document.DoubleField;
import tools.jackson.databind.ObjectMapper;

//TIP 要<b>运行</b>代码，请按 <shortcut actionId="Run"/> 或
// 点击装订区域中的 <icon src="AllIcons.Actions.Execute"/> 图标。
public class Main {
    public static void main(String[] args) {
        DoubleField field = new DoubleField("gg", 3.14, org.apache.lucene.document.Field.Store.YES);
        ObjectMapper mapper = new ObjectMapper();
        try {
            String json = mapper.writeValueAsString(field);
            System.out.println(json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
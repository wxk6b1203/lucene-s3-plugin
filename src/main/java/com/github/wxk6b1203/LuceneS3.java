package com.github.wxk6b1203;

import com.github.wxk6b1203.cli.Cli;
import com.github.wxk6b1203.cli.Server;
import picocli.CommandLine;

//TIP 要<b>运行</b>代码，请按 <shortcut actionId="Run"/> 或
// 点击装订区域中的 <icon src="AllIcons.Actions.Execute"/> 图标。
public class LuceneS3 {
    public static void main(String[] args) {
        picocli.CommandLine cmd  = new CommandLine(new Cli());
        cmd.execute(args);
    }
}
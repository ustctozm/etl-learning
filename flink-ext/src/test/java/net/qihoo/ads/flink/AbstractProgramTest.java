package net.qihoo.ads.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuming
 * @date 2022/2/24 11:24 上午
 */
public class AbstractProgramTest {

    @Test
    public void testDescription() throws ProgramInvocationException {
        String[] args = new String[] {"--env", "test", "--type", "fee"};
        List<URL> classPaths = new ArrayList<>();
        classPaths.add(FlinkMainClassDemo.class.getResource("/"));
        PackagedProgram packagedProgram = PackagedProgram.newBuilder().setUserClassPaths(classPaths).setArguments(args)
                .setEntryPointClassName(FlinkMainClassDemo.class.getName()).build();
        String description = packagedProgram.getDescription();
        Assert.assertEquals(description, FlinkMainClassDemo.class.getName());
    }
}
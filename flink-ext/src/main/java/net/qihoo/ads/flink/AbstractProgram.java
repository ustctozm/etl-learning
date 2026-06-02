package net.qihoo.ads.flink;

import java.net.URL;
import java.util.HashMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;

/**
 * @author zhuming
 * @date 2022/1/18
 */
public class AbstractProgram implements ProgramDescription {

  private static final class Singleton extends SecurityManager {
    public static Class<?>[] getSingletonClassContext() {
      return new Singleton().getClassContext();
    }
  }

  public Attributes getJarAttributes() {
    URL manifestPath = this.getClass().getResource("/META-INF/MANIFEST.MF");
    if (manifestPath == null){
      throw new RuntimeException("path: /META-INF/MANIFEST.MF does not exist!");
    }
    else {
      try {
        Manifest manifest = new Manifest(manifestPath.openStream());
        return manifest.getMainAttributes();
      } catch (Exception e) {
        throw new RuntimeException("parser manifest error: " + e.getMessage());
      }
    }
  }

  public static Attributes staticGetJarAttributes() {
    URL manifestPath = null;
    Class<?> mainClass = getFlinkMainClass();
    if (mainClass != null) {
      manifestPath = mainClass.getResource("/META-INF/MANIFEST.MF");
    }
    if (manifestPath == null) {
      throw new RuntimeException("path: /META-INF/MANIFEST.MF does not exist!");
    }
    else {
      try {
        Manifest manifest = new Manifest(manifestPath.openStream());
        return manifest.getMainAttributes();
      } catch (Exception e) {
        throw new RuntimeException("parser manifest error: " + e.getMessage());
      }
    }
  }

  public static ParameterTool addArgs(String[] args) {
    return ParameterTool.fromArgs(args);
  }

  private static ParameterTool addGitManifests() {
    HashMap<String, String> mfMap = new HashMap<>();
    Attributes attributes = staticGetJarAttributes();
    String implVerManifest = attributes.getValue("Implementation-Version");
    String versionManifest = attributes.getValue("version");
    String urlManifest = attributes.getValue("URL");
    String branchManifest = attributes.getValue("X-Git-Branch");
    String version = implVerManifest == null || implVerManifest.isEmpty() ? versionManifest : implVerManifest;
    if (StringUtils.isNotEmpty(version)) {
      mfMap.put("source-version", version);
    }
    if (StringUtils.isNotEmpty(urlManifest)) {
      mfMap.put("source-url", urlManifest);
    }
    if (StringUtils.isNotEmpty(branchManifest)) {
      mfMap.put("source-branch", branchManifest);
    }
    return ParameterTool.fromMap(mfMap);
  }

  public static Class<?> getFlinkMainClass() {
    Class<?> mainClass = null;
    Class<?>[] classList =  Singleton.getSingletonClassContext();
    if (classList != null) {
      for (int flinkMainClassPoint = 1; flinkMainClassPoint < classList.length; flinkMainClassPoint++) {
        //mainClass is before class "org.apache.flink.client.program.PackagedProgram"
        if (classList[flinkMainClassPoint].getName().equals("org.apache.flink.client.program.PackagedProgram")) {
          mainClass = classList[flinkMainClassPoint - 1];
          break;
        }
      }
    }
    return mainClass;
  }

  public static ParameterTool addFlinkMainClass() {
    HashMap<String, String> mfMap = new HashMap<>();
    Attributes attributes = staticGetJarAttributes();
    Class<?> mainClass = getFlinkMainClass();
    if (mainClass != null) {
      mfMap.put("entry-class", mainClass.getName());
    }
    else {
      String manifestClassName = attributes.getValue("Main-Class");
      if (StringUtils.isNotEmpty(manifestClassName)) {
        mfMap.put("entry-class", manifestClassName);
      }
    }
    return ParameterTool.fromMap(mfMap);
  }

  public static ParameterTool getParameterTool(String[] args)  {
    String allArgs = String.join(",", args);
    ParameterTool argList = ParameterTool.fromMap(new HashMap<String, String>() { { put("programArg", allArgs); } });
    ParameterTool parameterArgs = addArgs(args);
    ParameterTool parameterManifest = addGitManifests();
    ParameterTool parameterMainClass = addFlinkMainClass();
    return parameterArgs.mergeWith(parameterManifest).mergeWith(parameterMainClass).mergeWith(argList);
  }

  public static StreamExecutionEnvironment getJavaStreamExecutionEnvironment(String[] args) {
    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(getParameterTool(args));
    return env;
  }


  public static ExecutionEnvironment getJavaBatchExecutionEnvironment(String[] args) {
    org.apache.flink.api.java.ExecutionEnvironment env = org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(getParameterTool(args));
    return env;
  }

  @Override
  public String getDescription() {
    Attributes attributes = getJarAttributes();
    String revFromManifest = attributes.getValue("X-Git-Revision");
    String versionManifest = attributes.getValue("version");
    String implVerManifest = attributes.getValue("Implementation-Version");
    String titleManifest = attributes.getValue("Implementation-Title");
    String urlManifest = attributes.getValue("URL");
    String tagManifest = attributes.getValue("X-Git-Tag");
    String branchManifest = attributes.getValue("X-Git-Branch");

    String version = StringUtils.isEmpty(implVerManifest) ? versionManifest : implVerManifest;
    String rev = StringUtils.isEmpty(revFromManifest) ? "" : revFromManifest.substring(0, Math.min(8, revFromManifest.length()));

    return   (StringUtils.isEmpty(titleManifest) ? this.getClass().getName() : titleManifest)
           + (StringUtils.isEmpty(version) ? "" : " version=" + version)
           + (StringUtils.isEmpty(tagManifest) ? "" : " tag=" + tagManifest)
           + (StringUtils.isEmpty(branchManifest) ? "": " branch=" + branchManifest)
           + (rev.isEmpty() ? "" : " rev=" + rev)
           + (StringUtils.isEmpty(urlManifest) ? "" : " url=" + urlManifest);
  }
}

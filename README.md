mmall_learning

mvn clean package -Dmaven.test.skip=true -Pdev
mvn clean package -Dmaven.test.skip=true -Pbeta
mvn clean package -Dmaven.test.skip=true -Pprod

<build>
    <finalName>mmall</finalName>
    <plugins>
      <plugin>
        <groupId>org.mybatis.generator</groupId>
        <artifactId>mybatis-generator-maven-plugin</artifactId>
        <version>1.3.2</version>
        <configuration>
          <verbose>true</verbose>
          <overwrite>true</overwrite>
        </configuration>
      </plugin>

      <!-- geelynote maven的核心插件之-complier插件默认只支持编译Java 1.4，因此需要加上支持高版本jre的配置，在pom.xml里面加上 增加编译插件 -->
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <configuration>
          <source>1.7</source>
          <target>1.7</target>
          <encoding>UTF-8</encoding>
          <compilerArguments>
            <extdirs>${project.basedir}/src/main/webapp/WEB-INF/lib</extdirs>
          </compilerArguments>
        </configuration>
      </plugin>
    </plugins>

    <resources>
      <resource>
        <directory>src/main/resources.${deploy.type}</directory>
        <excludes>
          <exclude>*.jsp</exclude>
        </excludes>
      </resource>
      <resource>
        <directory>src/main/resources</directory>
      </resource>
    </resources>
  </build>

  <profiles>
    <profile>
      <id>dev</id>
      <activation>
        <activeByDefault>true</activeByDefault>
      </activation>
      <properties>
        <deploy.type>dev</deploy.type>
      </properties>
    </profile>
    <profile>
      <id>beta</id>
      <properties>
        <deploy.type>beta</deploy.type>
      </properties>
    </profile>
    <profile>
      <id>prod</id>
      <properties>
        <deploy.type>prod</deploy.type>
      </properties>
    </profile>
  </profiles>


  *id为表A的主键
  无锁
  select * from A where a.id = {id},a为空
  行锁
  select * from A where a.id = {id},a不为空
  表锁
  select * from A
  select * from A where a.id <> {id}
  select * from A where a.id like 'id'
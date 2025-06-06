
FROM eclipse-temurin:17-jre as runtime

WORKDIR /system

COPY target/tentris-odin-1.0-SNAPSHOT.jar my-system-adapter.jar

CMD ["java", "-cp", "my-system-adapter.jar", "org.hobbit.core.run.ComponentStarter", "org.hobbit.MySystemAdapter"]
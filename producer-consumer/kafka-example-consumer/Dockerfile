# Используем официальный образ Amazon Corretto 8
FROM amazoncorretto:8

# Устанавливаем Maven
RUN yum update -y && \
    yum install -y maven && \
    yum clean all

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файлы сборки (pom.xml и src) в рабочую директорию
COPY pom.xml .
COPY src ./src

# Собираем проект с помощью Maven
RUN mvn clean package -DskipTests

# Определяем команду для запуска приложения
CMD ["java", "-jar", "target/consumer-app.jar"]
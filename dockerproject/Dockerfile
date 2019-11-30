FROM java:8 
WORKDIR /
ADD part1.jar part1.jar
RUN bash -c 'touch /part1.jar'
ENTRYPOINT ["java","-jar","/part1.jar"]
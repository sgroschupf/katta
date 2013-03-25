# adding indexing sample jar
for i in `find build/artifacts -name "*.jar"`
do
  echo "adding to classpath $i ..."
  CLASSPATH=${CLASSPATH}:$i
done

#adding hadoop libs
for i in `find ../katta-core/lib/compile/hadoop -name "*.jar"`
do
  echo "adding to classpath $i ..."
  CLASSPATH=${CLASSPATH}:$i
done

$JAVA_HOME/bin/java -cp ${CLASSPATH} "$@"

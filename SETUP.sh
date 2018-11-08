cd mason
./SETUP.sh
mvn package
mvn install
cd ..
ln -s $(pwd)/mason/target/mason*.jar $(pwd)/contrib/geomason/local-repo/mason.jar
cd contrib/geomason
./SETUP.sh
mvn package
mvn install

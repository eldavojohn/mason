mvn install:install-file -Dfile=local-repo/jogamp-fat.jar -DgroupId=Jogamp -DartifactId=jogamp-fat -Dversion=9999 -Dpackaging=jar
mvn install:install-file -Dfile=local-repo/mpi.jar -DgroupId=mpi -DartifactId=mpi -Dversion=9999 -Dpackaging=jar
mvn install:install-file -Dfile=local-repo/portfolio.jar -DgroupId=portfolio -DartifactId=portfolio -Dversion=22 -Dpackaging=jar
# Come up with something for this...
mvn install:install-file -Dfile=local-repo/mason.jar -DgroupId=edu.gmu.cs -DartifactId=mason -Dversion=19 -Dpackaging=jar

### MASON Makefile
#### By Sean Luke

#### Relevant Stuff:
#### To see all your make options:  type   make help
#### To add flags (like -O) to javac:  change the FLAGS variable below
LIBS=`function join() { local IFS=$$1; shift; echo "$$*"; }; join ":" library/* `
FLAGS = -target 1.8 -source 1.8 -g -nowarn -cp ${LIBS}:$$CLASSPATH
JAVAC = mpijavac ${FLAGS}
BUILDDIR=$$(pwd)
VERSION = 19


# Main java files, not including the 3D stuff
DIRS = \
src/main/java/ec/util/*.java \
src/main/java/sim/app/heatbugs/*.java\
src/main/java/sim/app/dheatbugs/*.java\
src/main/java/sim/app/hexabugs/*.java \
src/main/java/sim/app/asteroids/*.java \
src/main/java/sim/app/antsforage/*.java \
src/main/java/sim/app/virus/*.java \
src/main/java/sim/app/cto/*.java \
src/main/java/sim/app/woims/*.java \
src/main/java/sim/app/mav/*.java \
src/main/java/sim/app/mousetraps/*.java \
src/main/java/sim/app/networktest/*.java \
src/main/java/sim/app/keepaway/*.java \
src/main/java/sim/app/lsystem/*.java \
src/main/java/sim/app/lightcycles/*.java \
src/main/java/sim/app/swarmgame/*.java \
src/main/java/sim/app/flockers/*.java \
src/main/java/sim/app/dflockers/*.java \
src/main/java/sim/app/tutorial1and2/*.java \
src/main/java/sim/app/tutorial3/*.java \
src/main/java/sim/app/tutorial4/*.java \
src/main/java/sim/app/tutorial5/*.java \
src/main/java/sim/app/schelling/*.java \
src/main/java/sim/app/pacman/*.java \
src/main/java/sim/app/pso/*.java \
src/main/java/sim/app/wcss/tutorial0*/*.java \
src/main/java/sim/app/wcss/tutorial10/*.java \
src/main/java/sim/app/wcss/tutorial11/*.java \
src/main/java/sim/app/wcss/tutorial12/*.java \
src/main/java/sim/app/wcss/tutorial13/*.java \
src/main/java/sim/display/*.java \
src/main/java/sim/engine/*.java \
src/main/java/sim/util/*.java \
src/main/java/sim/util/media/*.java \
src/main/java/sim/util/media/chart/*.java \
src/main/java/sim/util/gui/*.java \
src/main/java/sim/util/distribution/*.java \
src/main/java/sim/field/*.java \
src/main/java/sim/field/grid/*.java \
src/main/java/sim/field/continuous/*.java \
src/main/java/sim/field/network/*.java \
src/main/java/sim/field/storage/*.java \
src/main/java/sim/portrayal/*.java \
src/main/java/sim/portrayal/grid/*.java \
src/main/java/sim/portrayal/continuous/*.java \
src/main/java/sim/portrayal/network/*.java \
src/main/java/sim/portrayal/simple/*.java \
src/main/java/sim/portrayal/inspector/*.java 


# The additional 3D java files
3DDIRS = \
src/main/java/sim/app/heatbugs3d/*.java \
src/main/java/sim/app/woims3d/*.java \
src/main/java/sim/app/mousetraps3d/*.java \
src/main/java/sim/app/crowd3d/*.java \
src/main/java/sim/app/balls3d/*.java \
src/main/java/sim/app/celegans/*.java \
src/main/java/sim/app/particles3d/*.java \
src/main/java/sim/app/pso3d/*.java \
src/main/java/sim/app/tutorial6/*.java \
src/main/java/sim/app/tutorial7/*.java \
src/main/java/sim/app/celegans/*.java \
src/main/java/sim/app/wcss/tutorial14/*.java \
src/main/java/sim/portrayal3d/*.java \
src/main/java/sim/portrayal3d/simple/*.java \
src/main/java/sim/portrayal3d/grid/*.java \
src/main/java/sim/portrayal3d/grid/quad/*.java \
src/main/java/sim/portrayal3d/continuous/*.java \
src/main/java/sim/portrayal3d/network/*.java \
src/main/java/sim/display3d/*.java 


# Make the main MASON code, not including 3D code
all: SHELL=/bin/bash
all:
	@ echo This makes the 2D MASON code.
	@ echo To learn about other options, type 'make help'
	@ echo 
	${JAVAC} ${DIRS}

# Make the main MASON code AND the 3D code
3d: SHELL=/bin/bash
3d:
	${JAVAC} ${DIRS} ${3DDIRS}


# Delete all jmf gunk, checkpoints, backup emacs gunk classfiles,
# documentation, and odd MacOS X poops
clean:
	find . -name "*.class" -exec rm -f {} \;
	find . -name "jmf.log" -exec rm -f {} \;
	find . -name ".DS_Store" -exec rm -f {} \; 
	find . -name "*.checkpoint" -exec rm -f {} \;
	find . -name "*.java*~" -exec rm -f {} \;
	find . -name ".#*" -exec rm -rf {} \;
	rm -rf jar/*.jar docs/classdocs/resources docs/classdocs/ec docs/classdocs/src/main/java/sim docs/classdocs/*.html docs/classdocs/*.css docs/classdocs/package*


# Build the class docs.  They're located in docs/classdocs
doc:
	javadoc -classpath . -protected -d docs/classdocs src/main/java/sim.display src/main/java/sim.engine src/main/java/sim.util src/main/java/sim.util.gui src/main/java/sim.util.media src/main/java/sim.util.media.chart src/main/java/sim.field src/main/java/sim.field.grid src/main/java/sim.field.continuous src/main/java/sim.field.network src/main/java/sim.portrayal src/main/java/sim.portrayal.grid src/main/java/sim.portrayal.continuous src/main/java/sim.portrayal.network src/main/java/sim.portrayal.src/main/java/simple ec.util src/main/java/sim.portrayal3d src/main/java/sim.portrayal3d.grid src/main/java/sim.portrayal3d.continuous src/main/java/sim.portrayal3d.src/main/java/simple src/main/java/sim.portrayal3d.grid.quad src/main/java/sim.display3d src/main/java/sim.util.distribution

docs: doc

# Build an applet jar file.  Note this collects ALL .class, .png, .jpg, index.html, and src/main/java/simulation.classes
# files.  you'll probably want to strip this down some.
jar: SHELL=/bin/bash
jar: 3d
	touch /tmp/manifest.add
	rm /tmp/manifest.add
	echo "Main-Class: src/main/java/sim.display.Console" > /tmp/manifest.add
	echo "" >> /tmp/manifest.add
	echo $$(cat /tmp/manifest.add)
	#jar -cvfm jar/mason.${VERSION}.jar /tmp/manifest.add  `find library -name "*.jar"` `find . -name "*.class"` `find src/main/java/sim -name "*.jpg"` `find src/main/java/sim -name "*.png"` `find src/main/java/sim -name "*.pbm"` `find src/main/java/sim -name "index.html"` src/main/java/sim/display/src/main/java/simulation.classes src/main/java/sim/portrayal/inspector/propertyinspector.classes
	jar -cvfm jar/mason.${VERSION}.jar /tmp/manifest.add library/* src/* docs/* start/* 
	rm /tmp/manifest.add


# Build a distribution.  Cleans, builds 3d, then builds docs, then
# removes SVN directories
dist: clean 3d indent doc jar
	@ echo If the version is being updated, change it here:
	@ echo "1. sim.engine.SimState.version()"
	@ echo "2. manual.tex frontmatter (including copyright year)"
	@ echo "3. Makefile VERSION variable"
	touch TODO
	rm TODO
	find . -name ".svn" -exec rm -rf {} \;
	@ echo "If there were SVN directories, expect this to end in an error."
	@ echo "Don't worry about it, things are still fine."


# Indent to your preferred brace format using emacs.  MASON's default
# format is Whitesmiths at 4 spaces.  Yes, I know.  Idiosyncratic.
# Anyway, beware that this is quite slow.  But it works!
indent: 
	touch ${HOME}/.emacs
	find . -name "*.java" -print -exec emacs --batch --load ~/.emacs --eval='(progn (find-file "{}") (mark-whole-buffer) (setq indent-tabs-mode nil) (untabify (point-min) (point-max)) (indent-region (point-min) (point-max) nil) (save-buffer))' \;


# Print a help message
help: 
	@ echo MASON Makefile options
	@ echo 
	@ echo "make          Builds the model core, utilities, and 2D code/apps only"
	@ echo "make all        (Same thing)"
	@ echo "make 3d       Builds the model core, utilities, and both 2D and 3D code/apps"
	@ echo "make docs     Builds the class documentation, found in docs/classsdocs"
	@ echo "make doc        (Same thing)"
	@ echo "make clean    Cleans out all classfiles, checkpoints, and various gunk"
	@ echo "make dist     Does a make clean, make docs, and make 3d, then deletes SVN dirs"
	@ echo "make jar      Makes 3d, then collects ALL class files into a jar file"
	@ echo "              called mason.jar.  Heavyweight -- all class files included."

	@ echo "make help     Brings up this message!"
	@ echo "make indent   Uses emacs to re-indent MASON java files as you'd prefer"


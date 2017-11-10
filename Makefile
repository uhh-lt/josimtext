build:
	./sbt/sbt 'set test in assembly := {}' clean assembly

build-with-tests:
	./sbt/sbt clean assembly

tests:
	./sbt/sbt "testOnly -- -l NeedsMissingFiles"

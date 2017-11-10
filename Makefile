build:
	./sbt/sbt 'set test in assembly := {}' assembly

build-with-tests:
	./sbt/sbt clean assembly

tests:
	./sbt/sbt "testOnly -- -l NeedsMissingFiles"

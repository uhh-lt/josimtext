install:
	./sbt/sbt clean assembly

assembly-without-tests:
	./sbt/sbt 'set test in assembly := {}' assembly

test:
	./sbt/sbt clean test
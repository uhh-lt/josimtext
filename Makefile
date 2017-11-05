install:
	./sbt/sbt clean assembly

assembly-without-tests:
	./sbt/sbt 'set test in assembly := {}' assembly

test:
	./sbt/sbt "testOnly -- -l NeedsMissingFiles"
	# For an explanation of this command,
	# see the section 'Include and Exclude Tests with Tags'
	# in http://www.scalatest.org/user_guide/using_scalatest_with_sbt
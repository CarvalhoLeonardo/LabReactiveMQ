# LabReactiveMQ
Laboratory for a Reactive MQ messaging infra structure

## Pre requisites


*   Maven 3.5+
*   Java 10.



## Running.

With Maven, run this way :

	` mvn clean compile exec:java -Dexec.mainClass=leonardo.github.study.reactivemq.ReactiveMQ -Dexec.args="<thread count> <messages to send> <latency between messages>"` 
	
If there's too much logging to your taste, add `-Dloglevel=<LOG LEVEL>` to the command line.
package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;

// Any other imports that you may need.
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.logging.*;
import java.net.UnknownHostException;
import java.util.stream.Collectors;


// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.

// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos
{
	GCL gcl;
	FailCheck failCheck;
	int processId;
	int ballotId;
	String myProcess;
	String[] allGroupProcesses;

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		this.myProcess = myProcess;

		// sorts processes by hashcodes of host, port and uses indices to assign proc nums
		// should prevent collisions, assumes all processes are known at start
		List<Integer> hashCodes = Arrays.stream(allGroupProcesses).map(s -> s.hashCode())
				.collect(Collectors.toList());
		hashCodes.sort(Integer::compareTo);
		// check no collisions (repeated ids)
		assert(hashCodes.stream().collect(Collectors.toSet()).size() == hashCodes.size());
		this.processId = hashCodes.indexOf(myProcess.hashCode());

		this.ballotId = processId;
		this.allGroupProcesses = allGroupProcesses;
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

	}

	public boolean propose() {
		int maxBallot = ballotId;
		try {
			gcl.broadcastMsg(new GCMessage(this.myProcess, new PaxosMessage(MessageType.PROPOSE, ballotId)));
		} catch (NotSerializableException e) {
			System.err.println("Unable to serialize message. Skipping");
			e.printStackTrace();
		}
		try {
			int promises = 0;
			for (int i = 0; i < allGroupProcesses.length; i++) {
				GCMessage promise = gcl.readGCMessage();
				PaxosMessage paxosPromise = (PaxosMessage) promise.val;
				if ((int) paxosPromise.msg == ballotId) promises++;
				else if ((int) paxosPromise.msg > ballotId && (int) paxosPromise.msg > maxBallot) {
					maxBallot = (int) paxosPromise.msg;
				}
				if (promises > allGroupProcesses.length / 2) {
					return true;
				}
			}
		} catch (InterruptedException e) {
			// TODO: handle shutdown while blocking
		}
		ballotId = (maxBallot / allGroupProcesses.length + 1) * allGroupProcesses.length + processId;
		return false;
	}
	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val)
	{
		// This is just a place holder.
		// Extend this to build whatever Paxos logic you need to make sure the messaging system is total order.
		// Here you will have to ensure that the CALL BLOCKS, and is returned ONLY when a majority (and immediately upon majority) of processes have accepted the value.
		if (propose()) {
			// TODO: send accept?
		}
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		GCMessage gcmsg = gcl.readGCMessage();
		PaxosMessage paxosMessage = (PaxosMessage) gcmsg.val;
		switch(paxosMessage.type){
			case PROPOSE:
				if ((double) paxosMessage.msg > ballotId) {
					try {
						gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.PROMISE, ballotId)),
								gcmsg.senderProcess);
					} catch (NotSerializableException e) {
						throw new RuntimeException(e);
					}
				}
			case VALUE:

		}
		return gcmsg.val;
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		gcl.shutdownGCL();
	}

	private enum MessageType {
		PROPOSE, ACCEPT, VALUE, PROMISE
	}
	private class PaxosMessage implements Serializable {
		MessageType type;
		Object msg;

		PaxosMessage(MessageType type, Object msg) {
			this.type = type;
			this.msg = msg;
		}
	}
}


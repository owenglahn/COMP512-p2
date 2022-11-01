package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;

// Any other imports that you may need.
import java.io.*;
import java.util.ArrayList;
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
	int maxBallotIdSeen;
	int numProcesses;
	String myProcess;
	String[] allGroupProcesses;
	int numRefuse;
	int numAccept;
	int acceptedValue;
	ArrayList<PaxosMessage> deliverableMessages;


	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		this.myProcess = myProcess;

		// sorts processes by hashcodes of host, port and uses indices to assign proc nums
		// should prevent collisions, assumes all processes are known at start

		// get all hashcodes for processes and check no collisions
		List<Integer> hashCodes = Arrays.stream(allGroupProcesses).map(s -> s.hashCode())
				.collect(Collectors.toList());
		hashCodes.sort(Integer::compareTo);
		// check no collisions (repeated ids)
		assert(hashCodes.stream().collect(Collectors.toSet()).size() == hashCodes.size());

		this.processId = hashCodes.indexOf(myProcess.hashCode());

		// initial ballotID
		this.ballotId = processId;
		this.maxBallotIdSeen = 0;

		this.numProcesses = hashCodes.size();

		this.allGroupProcesses = allGroupProcesses;
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

	}

	private int computeNextBallotID() {
		int nextBallotID = maxBallotIdSeen;
		while (nextBallotID % numProcesses != processId) {
			nextBallotID += 1;
		}

		// TODO: this might be sus
		maxBallotIdSeen = nextBallotID;

		return nextBallotID;
	}

//	public boolean propose() {
//		// every replica must have a unique ballotId, between 0 and n-1
//		// select the smallest sequence number s that is larger than any sequence number seen so far
//		// s mod n = sequence number
//
//		int ballotId = computeNextBallotID();
//
//		try {
//			// send propose with ballot ID
//			gcl.broadcastMsg(new GCMessage(this.myProcess, new PaxosMessage(MessageType.PROPOSE, ballotId)));
//			this.numRefuse = 0;
//			this.numAccept = 0;
//		} catch (NotSerializableException e) {
//			System.err.println("Unable to serialize message. Skipping");
//			e.printStackTrace();
//		}
//		// TODO: can probably remove all this
//		try {
//			int promises = 0;
//			for (int i = 0; i < allGroupProcesses.length; i++) {
//				GCMessage promise = gcl.readGCMessage();
//				PaxosMessage paxosPromise = (PaxosMessage) promise.val;
//				if ((int) paxosPromise.msg == ballotId) promises++;
//				else if ((int) paxosPromise.msg > ballotId && (int) paxosPromise.msg > maxBallot) {
//					maxBallot = (int) paxosPromise.msg;
//				}
//				if (promises > allGroupProcesses.length / 2) {
//					return true;
//				}
//			}
//		} catch (InterruptedException e) {
//			// TODO: handle shutdown while blocking
//		}
//		ballotId = (maxBallot / allGroupProcesses.length + 1) * allGroupProcesses.length + processId;
//		return false;
//	}
	public boolean promise() {
		// TODO
		return false;
	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val) throws InterruptedException {
		// TODO: we might need to handle the case
		// e.g process1 receives process1's own message
		// Potential solution: Use gcl.multicast() instead of gcl.broadcast()

		// propose phase
		int ballotId = computeNextBallotID();
		try {
			// send propose with ballot ID, nothing to piggyback
			gcl.broadcastMsg(new GCMessage(this.myProcess, new PaxosMessage(MessageType.PROPOSE, ballotId, null)));
			this.numRefuse = 0;
			this.numAccept = 0;
		} catch (NotSerializableException e) {
			System.err.println("Unable to serialize message. Skipping");
			e.printStackTrace();
		}
		// Extend this to build whatever Paxos logic you need to make sure the messaging system is total order.
		// Here you will have to ensure that the CALL BLOCKS, and is returned ONLY when a majority (and immediately upon majority) of processes have accepted the value.
		while (this.numAccept != (int) Math.floor(this.numProcesses/2.0) + 1) {
			// keep reading messages until we are ready done
			// TODO: catch this error and remove throws from method signature
			GCMessage gcmsg = gcl.readGCMessage(); // this call blocks if there is no message, it waits :) very nice
			PaxosMessage paxosMessage = (PaxosMessage) gcmsg.val;
			int receivedBallotId = (int) paxosMessage.msg;

			switch(paxosMessage.type){
				// receive propose
				case PROPOSE:
					if (receivedBallotId >= this.maxBallotIdSeen) {
						try {
							gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.PROMISE, receivedBallotId,
											acceptedValue)), gcmsg.senderProcess);
							// log ballotID in persistent memory
							this.maxBallotIdSeen = (int) paxosMessage.msg;
						} catch (NotSerializableException e) {
							throw new RuntimeException(e);
						}
					}
					else {
						// refuse and return the highest ballotId seen so far
						try {
							gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.REFUSE, this.maxBallotIdSeen, null)),
									gcmsg.senderProcess);
						} catch (NotSerializableException e) {
							throw new RuntimeException(e);
						}
					}
					// received a promise
				case PROMISE:
					// receive a majority of promises
					this.numAccept += 1;
					if (this.numAccept >= this.numProcesses / 2 + 1) {
						// TODO: if someone already accepted value of a proposer with smaller ballotID:
						// find the most recent value that any responding acceptor accepted
						// ask acceptor to accept this value
						if (this.maxBallotIdSeen < receivedBallotId){
							// piggy back and promise with the new value;
							// TODO:
						}
						else{
							//send accept with any value
							try {
								gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.ACCEPT, receivedBallotId)),
										gcmsg.senderProcess);
								// TODO: send this value to every process with its corresponding value + ballotID
							} catch (NotSerializableException e){
								throw new RuntimeException(e);
							}
						}
					}


				case REFUSE:
					this.numRefuse += 1;
					this.maxBallotIdSeen = receivedBallotId;
					if (this.numRefuse >= Math.floor(this.numProcesses) + 1) {
						// TODO: propose again with a new ballotID
						// TODO: handle ties
					}
				case VALUE:
					// TODO:update most recently accepted value

				case ACCEPT:
					if (receivedBallotId > maxBallotIdSeen) {
						// TODO: send accept to the process
					}
					else {
						//TODO: send deny
					}
				case DENY:
					// TOOD: deny
				case CONFIRM:

			}
		}
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		while (deliverableMessages.size() == 0) {
			// block the process with a while loop
		}
		PaxosMessage message = deliverableMessages.remove(0);
		return message.msg;
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		gcl.shutdownGCL();
	}

	private enum MessageType {
		PROPOSE, ACCEPT, VALUE, PROMISE, REFUSE, DENY, CONFIRM
	}
	private class PaxosMessage implements Serializable {
		MessageType type;
		Object msg;
		Object piggyback;

		PaxosMessage(MessageType type, Object msg, Object piggyback) {
			this.type = type;
			this.msg = msg;
			this.piggyback = piggyback;
		}
	}
}


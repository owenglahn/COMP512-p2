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
	int maxBallotAccepted;

	int numProcesses;
	String myProcess;
	String[] allGroupProcesses;

	int numRefuse;
	int numPromise;


	int numAccepted;
	int numDenied;

	Object acceptedValue;
	ArrayList<Object> deliverableMessages;
	ListenerThread listener;


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

		// create thread for listener
		listener = new ListenerThread();
		listener.start();

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

	private class ListenerThread extends Thread {
		public void run() {
			while (true) {
				GCMessage gcmsg = null; // this call blocks if there is no message, it waits :) very nice
				try {
					gcmsg = gcl.readGCMessage();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				// parse messages
				PaxosMessage paxosMessage = (PaxosMessage) gcmsg.val;
				int receivedBallotId = (int) paxosMessage.msg;
				Object piggyback = paxosMessage.piggyback;

				switch (paxosMessage.type) {
					// receive propose
					case PROPOSE:
						if (receivedBallotId >= maxBallotIdSeen) {
							try {
								gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.PROMISE, receivedBallotId,
										acceptedValue)), gcmsg.senderProcess);
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
							// log ballotID in persistent memory
							maxBallotIdSeen = (int) paxosMessage.msg;
						} else {
							// refuse and return the highest ballotId seen so far
							try {
								gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.REFUSE, maxBallotIdSeen, null)),
										gcmsg.senderProcess);
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						}
						// received a promise
					case PROMISE:
						numPromise += 1;
						// receive a majority of promises
						if (numPromise >= numProcesses / 2 + 1) {
							// if someone already accepted value of a proposer with smaller ballotID:
							if (maxBallotIdSeen < receivedBallotId) {
								// piggyback and send accept with the new value;
								try {
									gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.ACCEPT, receivedBallotId, piggyback)),
											gcmsg.senderProcess);
								} catch (NotSerializableException e) {
									e.printStackTrace();
								}
							} else {
								// send this value to every process with its corresponding value + ballotID
								try {
									gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.ACCEPT, receivedBallotId, piggyback)),
											gcmsg.senderProcess);
								} catch (NotSerializableException e) {
									e.printStackTrace();
								}
							}
						}
						// receive a refuse promise
					case REFUSE:
						numRefuse += 1;
						maxBallotIdSeen = Math.max(receivedBallotId, maxBallotIdSeen);
						if (numRefuse >= Math.floor(numProcesses) + 1) {
							// propose again with a new ballotID
							int ballotId = computeNextBallotID();
							try {
								gcl.broadcastMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.PROPOSE, ballotId, null)));
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
							numRefuse = 0;
							numAccepted = 0;
							numPromise = 0;
						}
						// receive a piggyback value
					case VALUE:
						acceptedValue = piggyback;
						// receive accept
					case ACCEPT:
						// if still has the latest accepted ballot
						if (receivedBallotId > maxBallotAccepted) {
							acceptedValue = piggyback;
							maxBallotAccepted = receivedBallotId;
							try {
								gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.ACCEPTED, receivedBallotId, piggyback)),
										gcmsg.senderProcess);
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						} else { // return deny
							try {
								gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.DENY, receivedBallotId, piggyback)),
										gcmsg.senderProcess);
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						}
					case ACCEPTED:
						numAccepted += 1;
						if (numAccepted >= Math.floor(numProcesses) + 1) {
							deliverableMessages.add(piggyback);
							try {
								gcl.broadcastMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.CONFIRM, ballotId, piggyback)));
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						}
					case DENY:
						// deny
						numDenied += 1;
						if (numDenied >= Math.floor(numProcesses) + 1) {
							// give up on the instance, restart a round
							// propose again with a new ballotID
							int ballotId = computeNextBallotID();
							try {
								gcl.broadcastMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.PROPOSE, ballotId, null)));
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
							numRefuse = 0;
							numAccepted = 0;
							numPromise = 0;
						}
					case CONFIRM:
						// put message into the message queue
						deliverableMessages.add(piggyback);
				}
			}
		}
	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val) {
		// propose phase
		try {
			// send propose with ballot ID, nothing to piggyback
			int ballotId = computeNextBallotID();
			gcl.broadcastMsg(new GCMessage(this.myProcess, new PaxosMessage(MessageType.PROPOSE, ballotId, null)));
			this.numRefuse = 0;
			this.numAccepted = 0;
			this.numPromise = 0;

		} catch (NotSerializableException e) {
			System.err.println("Unable to serialize message. Skipping");
			e.printStackTrace();
		}
		// Extend this to build whatever Paxos logic you need to make sure the messaging system is total order.
		// Here you will have to ensure that the CALL BLOCKS, and is returned ONLY when a majority (and immediately upon majority) of processes have accepted the value.
		while (this.numAccepted != (int) Math.floor(this.numProcesses/2.0) + 1) {
			// block this thread from executing unless we have result
		}
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		while (deliverableMessages.size() == 0) {
			// block the process with a while loop
		}
		return deliverableMessages.remove(0);
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		gcl.shutdownGCL();
	}

	private enum MessageType {
		PROPOSE, ACCEPT, VALUE, PROMISE, REFUSE, DENY, ACCEPTED, CONFIRM
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


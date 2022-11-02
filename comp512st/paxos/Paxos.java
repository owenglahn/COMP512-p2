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

	int numProcesses;
	String myProcess;
	String[] allGroupProcesses;

	int numRefuse;
	int numPromise;
	int numAccepted;
	int numDenied;

	ArrayList<Object> deliverableMessages;
	ListenerThread listener;

	int maxBallotIdSeen;
	int lastAcceptedID;
	Object defaultAcceptValue;


	// this paxos is a proposer
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
		return nextBallotID;
	}

	private class ListenerThread extends Thread {
		int promisedId = 0;
		int acceptedId = 0;
		Object proposedValue;
		Object acceptedValue;

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
				String receivedProcess = gcmsg.senderProcess;
				int receivedBallotId = paxosMessage.proposalID;
				int receivedAcceptId = paxosMessage.acceptedID;
				Object receivedAcceptedValue = paxosMessage.acceptedValue;

				switch (paxosMessage.type) {
					// receive propose
					case PROPOSE:
						failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE); //to be invoked immediately when a process receives propose message.
						// if receive your own message
						if (receivedProcess == myProcess) {
							this.promisedId = receivedBallotId;
							maxBallotIdSeen = promisedId;
							// promise on itself.
							numPromise += 1;
						}
						else { // if receive message from others
							if (receivedBallotId >= this.promisedId) {
								// log ballotID in persistent memory
								this.promisedId = receivedBallotId; // this is the max
								// update max ballot for compute next id
								maxBallotIdSeen = receivedBallotId;

								try {
									gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.PROMISE, receivedBallotId,
											this.acceptedId, this.acceptedValue)), gcmsg.senderProcess);
									// to be invoked immediately AFTER a process sends out its vote for leader election.
									failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
								} catch (NotSerializableException e) {
									e.printStackTrace();
								}
							} else {
								// refuse and return the highest ballotId seen so far
								try {
									gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.REFUSE, this.promisedId, this.acceptedId, this.acceptedValue)),
											gcmsg.senderProcess);
									// to be invoked immediately AFTER a process sends out its vote for leader election.
									failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
								} catch (NotSerializableException e) {
									e.printStackTrace();
								}
							}
						}
						// received a promise
					case PROMISE: // Note: promise is only sent to the proposer itself
						numPromise += 1;
						if (receivedAcceptId > lastAcceptedID) {
							lastAcceptedID = receivedAcceptId;
							if (receivedAcceptedValue != null){
								this.proposedValue = receivedAcceptedValue;
							}
						}
						// receive a majority of promises
						if (numPromise >= numProcesses / 2 + 1) {
							try {
								if (this.proposedValue != null) {
									gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.ACCEPT, this.promisedId, receivedAcceptId, this.proposedValue)),
											gcmsg.senderProcess);
								} else {
									gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.ACCEPT, this.promisedId, receivedAcceptId, defaultAcceptValue)),
											gcmsg.senderProcess);
								}
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						}
					// receive a refuse promise
					case REFUSE: // only the proposer would receive a refuse
						numRefuse += 1;
						if (numRefuse >= Math.floor(numProcesses) + 1) {
							// propose again with a new ballotID
							int ballotId = computeNextBallotID();
							// reset proposer variables
							numRefuse = 0;
							numAccepted = 0;
							numPromise = 0;
							numDenied = 0;
							try {
								gcl.broadcastMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.PROPOSE, ballotId, 0, null)));
								// to be invoked immediately AFTER a process sends out its proposal to become a leader.
								failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						}
					// receive accept
					case ACCEPT:
						// if still has the latest accepted ballot
						if (acceptedId > promisedId) {
							this.promisedId = receivedBallotId;
							this.acceptedValue = receivedAcceptedValue;
							this.acceptedId = receivedAcceptId;
							try {
								gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.ACCEPTED, this.acceptedId, this.acceptedId, this.acceptedValue)),
										gcmsg.senderProcess);
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						} else { // return deny
							try {
								gcl.sendMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.DENY, receivedBallotId, this.acceptedId, this.acceptedValue)),
										gcmsg.senderProcess);
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						}
					case ACCEPTED:
						if (numAccepted == numProcesses / 2) {
							failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);
							// to be invoked immediately AFTER a process sees that it has been accepted by the majority as the leader.
							deliverableMessages.add(acceptedValue);
							numAccepted += 1;
							try {
								gcl.broadcastMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.CONFIRM, receivedBallotId, acceptedId, acceptedValue)));
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						}
					case DENY:
						// deny
						numDenied += 1;
						if (numDenied >= numProcesses/2 + 1) {
							// give up on the instance, restart a round
							numRefuse = 0;
							numAccepted = 0;
							numPromise = 0;
							numDenied = 0;
							// propose again with a new ballotID
							try {
								gcl.broadcastMsg(new GCMessage(myProcess, new PaxosMessage(MessageType.PROPOSE, computeNextBallotID(), 0, null)));
								// to be invoked immediately AFTER a process sends out its proposal to become a leader.
								failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);
							} catch (NotSerializableException e) {
								e.printStackTrace();
							}
						}
					case CONFIRM:
						// put message into the message queue
						deliverableMessages.add(acceptedValue);
						failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
						// to be invoked immediately by the process once a majority has accepted it’s proposed value.
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
			maxBallotIdSeen = ballotId;
			defaultAcceptValue = val;
			gcl.broadcastMsg(new GCMessage(this.myProcess, new PaxosMessage(MessageType.PROPOSE, ballotId, 0, null)));
			// to be invoked immediately AFTER a process sends out its proposal to become a leader.
			failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);

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
		deliverableMessages = new ArrayList<>();
		gcl.shutdownGCL();
	}

	private enum MessageType {
		PROPOSE, ACCEPT, PROMISE, REFUSE, DENY, ACCEPTED, CONFIRM
	}
	private class PaxosMessage implements Serializable {
		MessageType type;
		int proposalID;
		int acceptedID;
		Object acceptedValue;

		PaxosMessage(MessageType type, int proposalID , int acceptedID, Object acceptedValue) {
			this.type = type;
			this.proposalID = proposalID;
			this.acceptedID = acceptedID;
			this.acceptedValue = acceptedValue;
		}
	}
}


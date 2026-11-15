# Simulation bench

Runs the real collector and its disk spool with a simulated fleet of robots whose producers resend, restart and fall silent, and a simulated data lake that flaps on a fixed seed.

Scenarios:

- twelve simulated robots through a data lake that goes down and up on a seeded schedule, with a spool: every accepted sample is delivered exactly once and in the order of its producer, resent messages are counted once, and nothing is dropped;
- the same without a spool: the buffer refuses instead of losing, and what was refused is accepted when the producers send it again;
- a restarted producer is accepted again and counted once;
- two hundred seeded messages of random bytes and ten malformed messages never reach the buffer or the lake.
- a collector that crashes while the lake is down and starts again from the same disk spool and the same saved state: what was on the disk is delivered, the messages the producers send again are counted as duplicates, and everything accepted is delivered once and in order;
- a producer that the lake rejects for good while the lake flaps for the others: only its samples are quarantined, one count each, and nothing else is dropped;
- a spool that fills up while the lake is down: new samples are refused instead of lost, a refused message is accepted when it is sent again, and nothing that was accepted is lost;
- the real sink against an HTTP twin of the data lake that answers 503 and 500, resets the connection, and stores a sample but loses the answer: the order of each producer is kept, nothing is lost, and a sample is stored more than once only for each answer that was lost after it was stored (delivery is at least once).

The evidence of this project is `simulator`: it says nothing about real machines, real networks or real load.

Physical tests still pending:

- a real HYDRA-UMC-DATALAKE and a real robot on the CAN bus: stop the data lake for a minute and start it again; expected: the samples of that minute appear in the lake in the order of the robot, with no gap and no repeated row;
- the same with the collector killed (`kill -9`) while the lake is down and started again; expected: the spooled samples are delivered once the lake is back and the robot's resent messages are not counted twice;
- a disk that fills up under a long outage on the CM5; expected: ingestion answers 503 and recovers by itself when the lake is back.

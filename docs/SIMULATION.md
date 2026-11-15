# Simulation bench

Runs the real collector and its disk spool with a simulated fleet of robots whose producers resend, restart and fall silent, and a simulated data lake that flaps on a fixed seed.

Scenarios:

- twelve simulated robots through a data lake that goes down and up on a seeded schedule, with a spool: every accepted sample is delivered exactly once and in the order of its producer, resent messages are counted once, and nothing is dropped;
- the same without a spool: the buffer refuses instead of losing, and what was refused is accepted when the producers send it again;
- a restarted producer is accepted again and counted once;
- two hundred seeded messages of random bytes and ten malformed messages never reach the buffer or the lake.

The evidence of this project is `simulator`: it says nothing about real machines, real networks or real load.

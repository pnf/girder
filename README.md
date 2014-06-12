# girder

Distributed cache and grid.

## Usage

Don't.

## Notes

### Grid

#### Hierarchy of peers.
Multi layer strategy.  Everybody is a worker.  Everyone maintains queue.
 A. Compute nodes
   1. Pull work only from own queue.
   2. Push reentrant requests to own queue.
   3  After making re-entrant request, work through own queue, then block for completion.
   3. Ignore work marked done or in progress.
   4. Volunteer at parent when free.
 B. Organizer nodes
   1. Pull work from own queue
   3. COPY work from member node queues to own after delay threshold, possibly zero.
   2. MOVE/push work to member node queues if they volunteer, possibly based on criteria, then unvolunteer them.
 C. Generative nodes
   1. Pull work from own queue
   2. Based on inspection of work, may create an appropriate volunteer.
   3. Maintains knowledge of what it pushed down to which volunteer, so it can monitor them and possibly service requests
      should the volunteer "quit".



(condp = (GET state)
  :unseen (do (queue local) (subscribe reqid))
  :inprogress (do (subscribe reqid) (check state again))
  :done (get it))

#### REDIS data structures

* req-set-PEERID
* req-queue-PEERID      - lpop/rpush REQID
* vol-queue-PEERID      - lpop/rpush PEERID
* members-NODEID
* chan-REQID         - publish STATE
* listeners-REQID    - sadd/srem PEERID
* value-REQID        - blob
* state-REQID        - if in progress, will include location



When finished computing:
(PUT reqid-val value)
(SET reqid-state :done)
(PUBLISH reqid "done")




## License

Copyright © 2014 Peter Fraenkel

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

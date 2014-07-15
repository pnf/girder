# girder

Distributed cache and grid.

## Usage

Don't.

## Notes

### Grid

A. Should operate at high throughput.  Aim for several ms overhead.
B. Calculations are dispatched as requests consisting of vectors ```[function arg1 arg2 ...]```.
C. Requests will be serialized, turning the function into a name-space qualified string and edenifying the
   arguments.  Obviously, work can only be done at remote nodes that have function in their classpath.
D. Fully support re-entrant requests, i.e. allow functions to make their own requests.  Doing this
   without risking grid "starvation" (where requests cannot be processed because all workers claim to be
   busy), but also efficiently and with predictable load.
E. Interface is generally via core.async.
F. An assumption throughout is that all requests are referentially transparent, and repeating the
   evaluation of one will at worst result in unnecessary work.


#### Hierarchy of peers.
Multi layer strategy.  All nodes maintain a queue.

A. Worker nodes
   1. Pull work only from own queueu.
   2. Push reentrant requests to own queue.
   3.  After making re-entrant request, work through own queue, then block for completion.
   4. Ignore work marked done or in progress.
   5. Volunteer at parent when free.
 B. Distributor nodes
   1. Pull work from own queue
   2. MOVE/push work to member node queues if they volunteer, possibly based on criteria, then unvolunteer them.
   3. Volunteer at parent when free
 C. Helper nodes
   1. COPY work up from members periodically, placing it at the end of our queue,
      so it may be distributed to a volunteer if one is ready before
      the worker we copied it from.
   2. Generally share a pool with a distributor, otherwise it's kind of pointless.

Typical use would be to make top-level requests at a distributor pool, where they would be doled out to
idle workers that had volunteered.  If workers themselves make requests, their queues may elongate, in which
case the helper might hoist them upwards.



## License

Copyright © 2014 Peter Fraenkel

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

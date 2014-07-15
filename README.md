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
   Both arguments and return value can be arbitrary Clojure values.  
D. Return values are cached.
E. Fully support re-entrant requests, i.e. allow functions to make their own requests.  Doing this
   without risking grid "starvation" (where requests cannot be processed because all workers claim to be
   busy), but also efficiently and with predictable load.
F. Interface is generally via core.async.
G. An assumption throughout is that all requests are referentially transparent, and repeating the
   evaluation of one will at worst result in unnecessary work.

#### Redis Back-end

There is an assumption of a central statekeeper back-end, of which multiple versions could exist, but
only Redis has yet been implemented.


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

#### Resiliance

As noted, all requests are supposed to be referentially transparent, so in the worst case we can simply
bounce everything.  That said, we would like to be able to kill and restart individual services with
minimal disruption.  Currently, it is still possible to lose volunteers and requests that have been pulled off
a redis queue onto an ```async``` channel, but not yet properly processed.

The standard Redis reliable queue strategies feel a little heavy-weight.  I'm trying to think of something that will
err on the side of retaining too many requests.

## License

Copyright © 2014 Peter Fraenkel

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

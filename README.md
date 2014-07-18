# girder

Distributed, high-performance calculation grid with for distributed
computation.

## Usage

Don't.

## Design goals and principles

* Support _emergent graphs_, i.e. computations running on the grid may
  request and use the results of other computations on the grid,
  but we don't claim to know the data-flow graph in advance.
* We must therefore fully support re-entrant requests.  When
  functions make their own requests, we must not risk grid
  "starvation" (where requests cannot be processed because all workers
  claim to be busy, when they are in fact in a wait state),
  but also efficiently and with predictable load.
* Should operate at high throughput.  Aim for several ms overhead.
* Requests should be built with ordinary clojure functions and values.
* Return values are cached, which implies anassumption 
  that all requests are referentially transparent, further implying:
  * repeating the evaluation of one will at worst
    result in unnecessary work
  * it is valid and safe to get back a cached result
* Interface is generally via ```core.async```.  Requests for computations
  return channels, which will eventually deliver results.
* We rely internally on ```async``` to coordinate data flow without directly
  playing with thread pools.
* There will be a central statekeeper (Redis, at this point),
  easing reliable coordination and facilitating reporting on the state of the system.


## Notes on strategy

* Worker nodes
   1. Pull work only from own queueu.
   2. Push reentrant requests to own queue.
   3. After making re-entrant request, work through own queue, then block for completion.
   4. Ignore work marked done or in progress.
   5. Volunteer at parent when free.
* Distributor nodes
   1. Pull work from own queue
   2. MOVE/push work to member node queues if they volunteer, possibly based on criteria, then unvolunteer them.
   3. Volunteer at parent when free
   4. COPY work up from members periodically (e.g. every 100ms), placing it at the end of our queue,
      so it may be distributed to a volunteer if one is ready before
      the worker we copied it from gets to it.  Work thus tends to remain local.

Typical use would be to make top-level requests at a distributor pool, where they would be doled out to
idle workers that had volunteered.  If workers themselves make requests, their queues may elongate, in which
case the helper might hoist them upwards.

## Usage, if you must

See ```acyclic.girder.testscripts.boffo``` for a working example within a single process (which is kind of
pointless, except for demonstration purposes.

### requests

A "request" is defined as a vector:
~~~.clj
  [fc arg1 arg2 ... argn]
~~~
where fc is a Clojure function that takes n arguments and returns an ```async`` channel that will eventually
return a result of the form

~~~.clj
  {:value RESULT
   :error OPTIONAL-ERROR}
~~~

The ```acyclic.girder.grid.async.cdefn``` macro facilitates writing
functions like this.  Essentially, you use it as if it were
```defn```, returning a plain result value.  It will all get wrapped
up properly, becoming a function that returns a channel that delivers
a map, even populating ```:error``` with a stack-trace and other
niceties if an exception is thrown somewhere.  Furthermore, all forms
will be executed within an ```async/go``` block, so you could do
something fancy if you wanted.  If, for example, a function were to
communicate with a non-JVM process, ```async``` might very well be
involved.  It is also reasonable for the function to interact with
databases of its own; for example, it might memoize its output in a
disk- and/or cluster-based store, and then return to girder only a UUID key.

The request is going to be serialized.  The function, in particular,
will be passed as a string containing its fully name-space qualified
name.  This string will be ```resolve```ed eventually, so the function
must exist in the JVM that processes the request.  No magic: you have
to distribute uberjars.  The remaining arguments will be
serialized/deserialized with ```pr-str```/```read-string```, so pretty
much any Clojure structures will work here, but they shouldn't contain
closures or other functions.

A request is submitted by
~~~.clj
  (enqueue POOLID request)
~~~
where we'll get to POOLID in just a minute.  This will return a channel that will eventually deliver the result in
the form discussed above.

## re-entrant requests

The functions just discussed are allowed to enqueue requests of their own:
~~~.clj
  (call-reentrant requests)
~~~
where the second argument is a sequence of requests in standard form.  This
"blocks" (in a ```go```-ish sense), eventually returning a sequence of results.
These are true results, not encased in ```{:value ...}```.  In the event of
an error from one of the re-entrant calls, the exception will be enriched
and rethrown such that the outermost ```cdefn``` ends up returning something of the form
~~~.clj
{:error {:msg "Error from request"
         :req "[originalfn 1 "blah"]
		 :info {[req-that-caused-error 2 "bleh"] {:stack [...]
		                                           :msg "Icky value"}
	            [another-bad-req 3 "blah"]       {:stack [...]
		                                         :msg "Smelly value"}}}}
~~~												 
Note that ```call-reentrant``` is a macro that does the ```<!``` wait and
deals with errors.  You could call the ```enqueue-reentrant``` function
directly and read from the channel it returns.


### Redis Back-end

There is an assumption of a central statekeeper back-end, of which
multiple versions could exist, but only Redis has yet been
implemented.

#### Resiliance

As noted, all requests are supposed to be referentially transparent,
so in the worst case we can simply bounce everything.  That said, we
would like to be able to kill and restart individual services with
minimal disruption.

An important internal function is ```crpop```, which essentially turns
a Redis queue into an ```async``` channel.  Once a request has been popped
from Redis and placed onto the channel, there is a potential hole, where
a process might go down before the request is processed.  For this reason,
```crpop``` uses Redis' ```BRPOPLPUSH```, which atomically pops from one
list and pushes onto another.  This "backup" list only gets cleared once
the request has been safely dealt with.

Untimely process deaths will result in prevalence of Redis keys like
```requests-queue-bak-poolname```, which contain the possibly unprocessed
requests.

There is not yet any automatic recovery, which would entail checking on
launch for backup queues and then transferring their content to request
queues.  This is coming soon....

Of course Redis itself might go down, but I've decided that this
presumably rare occurrence is sufficiently mitigated by referential
transparency.  In the event of an "outage," it may be necessary to resubmit
external requests to the system.

## License

Copyright © 2014 Peter Fraenkel

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.


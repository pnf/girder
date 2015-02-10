# girder

Distributed, high-performance calculation grid with for distributed
computation.

## Design goals and principles

See http://podsnap.com/girder.html, but note the comment at the top explaining that the distribution
and scheduling algorithm has changed a lot.

## Usage

Don't.

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


### Notes on traversal

#### BFS Algorithm in v1

Girder algorithm effectively performed a breadth-first search, with reentrant
requests going to the back of the work queue.  By contrast, Doug Lea's fork-join
framework does a depth-first search, pushing new requests to the top of a work
stack.  In FJ, idle workers steal jobs from the bottom of the work stack, which
tends to encourage distribution of jobs closer to the root.  Why don't we do the
same?

One consideration we have that FJ doesn't is result sharing.  Two requests may
each generate the same sub-request.  In the example below, workers 1 and 2 start,
respectively on requests A and Z.  Z requests E directly, while A requests it indirectly,
via several intermediate requests.  Suppose that, by the time D requests E, it's
already in progress at worker 2.  While waiting for E to complete, worker 1 polls
the stack and pulls C.  But C is waiting on D, which will never complete.

    :::text
      W1    W2
	   A     Z
       /\   /
      B  C /
       \/ /
	   D /
	   |/
	   E  <-- E is already in progress at another worker


| Local Stack | Work Stack | Action |
|-------------|------------|----- |
|             | A          | new work! |
| A           |            | pop A from work stack |
| A           | BC         | push B and C; wait for results |
| AB          | C          | pop B |
| AB          | DC         | push D |
| ABD         | C          | pop D |
| ABD         | EC         | push E |
| ABDE        | C          | pop E |
| ABD         | C          | ignore, as its already in progress |
| ABDC        |            | pop C

#### DFS Relay Algorithm in v2

Version 2 of the algorithm performs a DFS and solves the sharing deadlock
without using breadth-first-search.  We no longer have a Clojure call stack mirroring
the request call stack.  Rather, every request is evaluated in its own
```go``` block and control is transferred between active requests by means
of a special "baton" channel.  The baton is passed right before any potentially
parking operatation, and grabbed immediately afterwards.

The issuance of a reentrant request proceeds as follows:

1. Subscribe to the id of the request, receiving a channel on which notification will be sent.
2. Push the request onto the local stack.
3. Pass the baton.
4. Await notification
5. Grab baton.

Meanwhile (considering only local requests for now) the worker thread:

1. Passes the baton.
2. Pops a request, parking if none is available.
3. Grabs the baton.
4. Passes it to a new request processor, which immediately detaches into a go block.
5. Grabs the baton.

The processor:

1. Checks for cached value, and if found, publishes it and passes the baton and exits.
2. Checks for a request already running and, if found, passes the baton immediately,
   since whoever made the request is already signed up for notification, and exits.
3. Otherwise, launches the request evaluation into a go block and parks, waiting for completion.
4. And passes the baton.


Of course, between steps 3 and 4, the request evaluation may issue a reentrant request of its
own, in which case the baton will get passed waiting for its evaluation.

In this way, a single thread of execution is maintained.

While requests are popped from the top of the stack, work to be shared is dealt from
the bottom, thus (as in FJ) making it more likely that shared work is higher up the
call graph and hence more general.

Sharing occurs on a period schedule.  We keep track of how many requests were popped by
the worker in the last period and reduce its stack to that size.


## License

Copyright © 2014 Peter Fraenkel

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.


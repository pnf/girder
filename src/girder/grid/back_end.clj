(ns girder.grid.back-end)

(defprotocol Girder-Backend 

  (clean-all [this] "Clear out database, or whatever")

  (crpush [this key queue-type]
    "Push onto the main key value pub/sub queue, via the channel, which is returned.")
  (clpop [this key queue-type])
  (rpush [this key queue-type val])
  (rpush-many [this key queue-type vals])
  (rpush-and-set [this
                  qkey queue-type qval
                  vkey val-type vval])

  (get-members [this key set-type])
  (get-val [this key val-type])
  (set-val  [this key val-type val])
  (qall [this key :queue-type])
  (add-member [this key set-type mem])
  (remove-member [this key set-type mem])

  (kv-listen [this k])
  (kv-publish [this k v])
  (kv-close [this])

  (srem [this key val])

  (enqueue-listen
    [this
     queueid reqid 
     queue-type val-type
     enqueue-pred
     done-pred
     done-extract]
    "Places the req on a queue, returning a channel containing updates to the request's state.
Internally, the queue and request ids may be turned into specific keys for a queue, a publication
topic and a state variable.  The two predicates, if specified, determine based on the state, whether
something should be enqueued and whether the request should be considered complete."))

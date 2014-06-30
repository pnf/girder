(ns girder.back-end)

(defprotocol Girder-Backend 
  (crpush [this key queue-type]
    "Push onto the main key value pub/sub queue, via the channel, which is returned.")
  (clpop [this key queue-type])
  (rpush [this key queue-type val] [this key queue-type val & vals])
  (get-members [this key set-type])
  (get-state [this key])
  (set-state [this key val])
  (qall [this key :queue-type])
  (add-member [this key set-type mem])
  (remove-member [this key set-type mem])

  (kv-listen [this k])
  (kv-publish [this k v])
  (kv-close [this])

  (srem [this key val])

  (enqueue-listen
    [this
     queueid reqid & [enqueue-pred done-pred]]
4    "Places the req on a queue, returning a channel containing updates to the request's state.
Internally, the queue and request ids may be turned into specific keys for a queue, a publication
topic and a state variable.  The two predicates, if specified, determine based on the state, whether
something should be enqueued and whether the request should be considered complete."))

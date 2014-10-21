(ns acyclic.girder.grid.back-end)

(defprotocol Girder-Backend 
  (clean-all [this] "Clear out database, or whatever")
  (crpop [this key queue-type] "Return a channel that is fed from the specified queue.")
  (clpush [this key queue-type]  "Return a channel, writing to which will feed the specified queue.")
  (lpush [this key queue-type val] "Explicitly push val onto the specified queue.")
  (lpush-many [this key queue-type vals])
  (lpush-and-set-tag [this
                       qkey queue-type qval
                       vkey tag-type vtag]
    "Explicitly push qval onto the specified queue, atomically setting the specified scalar to vval.")
  (clear-bak [this qkeys-qtypes] "Clear the backups of elements popped from queues.")
  (get-members [this key set-type] "Return the members from the specified set.")
  (get-val [this key val-type] "Return the value from a specified key.")
  (set-val  [this key val-type val] "Set a value from the specified key, returning the old value.  Set nil to erase.")
  (set-tag [this key tag-type tag] "Set a value from the specified key, returning the old value.  Back-end attempts to maintain legibility of internal key Set nil to erase.")
  (get-tag [this key tag-type] "Return the value from a specified key.")
  (qall [this key :queue-type] "Fetch, but do not pop, all members of the specified queue.")
  (add-member [this key set-type mem])
  (remove-member [this key set-type mem])

  (kv-listen [this k debug-info] "Return a channel that will receive the next publication to the specified key.")
  (kv-publish [this k v] "Publish a value by the specified key.")
  (kv-close [this] "Shut down the internal listening process.  Probably don't do this.")

  (enqueue-listen
    [this
     queueid reqid 
     queue-type
     debug-info]
    "Places the request on a queue, returning a channel containing updates to the request's state.
Internally, the queue and request ids may be turned into specific keys for a queue and a publication topic."))

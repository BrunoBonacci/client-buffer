(ns client-buffer.core
  (:require [amalloy.ring-buffer :as rb]))

;; this is the ring-buffer size
;; typically should be read from a configuration
;; or default to 10000
(def BUFFER-SIZE 5)

;; this is atomic incremental counter
;; every event in the buffer is going
;; to have a unique number assigned
(def !counter! (atom 0))

;; ring-buffer which will contains the events
;; events will be added in a pair of unique id
;; and actual event (such as: [ 1 {:eventName "..."}]
;; the number will be used to remove the events
;; which have been sent
(def !buffer!  (atom (rb/ring-buffer BUFFER-SIZE)))


(defn next-number
  "It is a monotonic atomic counter which returns a unique number"
  []
  (swap! !counter! inc))


(defn enqueue!
  "add an event to the event buffer by assigning a unique id"
  [event]
  (let [id (next-number)]
    (swap! !buffer! into [[id event]])))


(defn events
  "Given a buffer it returns a sequence of events present in the buffer"
  [buffer]
  (map second buffer))


(defn pop-while
  "like drop-while but for ring-buffer"
  [pred buffer]
  (if (pred (peek buffer))
    (recur pred (pop buffer))
    buffer))


(defn drop-events
  "Given a list of [id events] will remove the events
   which are still in the buffer."
  [buffer events]
  (let [to-remove (set (map first events))]
    (pop-while (comp to-remove first) buffer)))


(defn dequeue!
  "Removes the given events from the ring-buffer."
  [events]
  (swap! !buffer! drop-events events))



(comment

  ;; you start with an empty buffer of size 5

  !buffer!
  ;;=> #<Atom@6d3e2e6e: ()>

  ;; let's try to add one element
  (enqueue! \a)

  !buffer!
  ;;=> #<Atom@70b387e6: ([1 \a])>

  ;; as you can see the event has bee added in pair with a incremental
  ;; atomic number

  ;; let's put 4 elements
  (doseq [e "bcde"]
    (enqueue! e))

  !buffer!
  ;;=> #<Atom@70b387e6: ([1 \a] [2 \b] [3 \c] [4 \d] [5 \e])>

  ;; let's try to add one more element
  (enqueue! \f)

  ;; now the buffer is full it contains 5 elements
  !buffer!
  ;;=> #<Atom@70b387e6: ([2 \b] [3 \c] [4 \d] [5 \e] [6 \f])>

  ;; as you can see the first element \a has been evicted
  ;; to make the space for the last element \f


  ;; now lets assume it's the time to send data to the server
  ;; we take a snapshot of the current buffer with deref or @
  (def to-send @!buffer!)

  ;; this is the current status of the buffer
  ;; saved in a immutable snapshot
  to-send
  ;;=> ([2 \b] [3 \c] [4 \d] [5 \e] [6 \f])

  ;; now let's assume we have two new values
  ;; coming into to the buffer while we are preparing th post
  (enqueue! \G)
  (enqueue! \H)

  ;; now the buffer status shows the new events
  !buffer!
  ;;=> #<Atom@70b387e6: ([4 \d] [5 \e] [6 \f] [7 \G] [8 \H])>

  ;; however our snapshot is still the same
  to-send
  ;;=> ([2 \b] [3 \c] [4 \d] [5 \e] [6 \f])

  ;; so we can work with our snapshot undisturbed.
  ;; we can retrieve the list of events with
  (events to-send)
  ;;=> (\b \c \d \e \f)

  ;; NOW WE ARE DOING THE POST
  ;; (publish-events (events to-send))

  ;; if everythings is ok we can now discard
  ;; these elements from the buffer
  ;; to do so, I remove all elements which have an
  ;; id (first number in the pair) which is contained in
  ;; the list of events we just sent.
  (dequeue! to-send)
  ;;=> ([7 \G] [8 \H])

  ;; and magically we have only the events which where
  ;; added after we captured the snapshot


  ;; so the overall function which send the data could look like
  (defn publish-buffer! []
    ;; taking a snapshot of the current buffer
    (let [to-send @!buffer!]
      ;; POSTing the data
      (publish-events (events to-send))
      ;; now remove the posted events from the buffer
      (dequeue! to-send)))

  )

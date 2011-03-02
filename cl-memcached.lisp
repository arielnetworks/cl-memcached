;;; -*- Mode: LISP; Syntax: COMMON-LISP; Package: CL-USER; Base: 10 -*-

;;; Copyright (c) 2006, Abhijit 'quasi' Rao.  All rights reserved.
;;; Copyright (c) 2006, Cleartrip Travel Services.

;;; Redistribution and use in source and binary forms, with or without
;;; modification, are permitted provided that the following conditions
;;; are met:

;;;   * Redistributions of source code must retain the above copyright
;;;     notice, this list of conditions and the following disclaimer.

;;;   * Redistributions in binary form must reproduce the above
;;;     copyright notice, this list of conditions and the following
;;;     disclaimer in the documentation and/or other materials
;;;     provided with the distribution.

;;; THIS SOFTWARE IS PROVIDED BY THE AUTHOR 'AS IS' AND ANY EXPRESSED
;;; OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
;;; WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
;;; ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
;;; DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
;;; DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
;;; GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
;;; INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
;;; WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
;;; NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
;;; SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

(in-package #:cl-memcached)

(defvar *memcache* 
  "Represents a particular Memcached server")

(defvar *use-pool* nil 
  "Default value for the USE-POOL keyword parameter in memcached functions")

(defvar *pool-get-trys?* nil
  "If true then it will try to wait and sleep for a while if pool item in unavailable,
if nil then will return immideatly")

(defvar *default-encoding* (flex:make-external-format :iso-8859-1)
  "Default encoding to be used on the flexi-stream used to read and write data")

(defstruct 
  (memcache-stats
   (:conc-name mc-stats-)
   (:print-function
    (lambda (struct stream depth)
      (declare (ignore depth))
      (format stream "#<MEMCACHED-SERVER-STATS PID:~A MAX-BYTES:~dMb CURR-ITEMS:~d TOT-ITEMS:~D>"
	      (mc-stats-pid struct)
	      (/ (mc-stats-limit-maxbytes struct) 1024 1024)
	      (mc-stats-curr-items struct)
	      (mc-stats-total-items struct)))))
"The structure which holds the statistics from the memcached server. The fields are :
field-name                 accessor-function                 documentation
----------                 -----------------                 -------------
pid                        mc-stats-pid                      Process id of this server process
uptime                     mc-stats-uptime                   Number of seconds this server has been running
time                       mc-stats-time                     current UNIX time according to the server
version                    mc-stats-version                  Version string of this server
rusage-user                mc-stats-rusage-user              Accumulated user time for this process
rusage-system              mc-stats-rusage-system            Accumulated system time for this process
curr-items                 mc-stats-curr-items               Current number of items stored by the server
total-items                mc-stats-total-items              Total number of items stored by this server ever since it started
bytes                      mc-stats-bytes                    Current number of bytes used by this server to store items
curr-connections           mc-stats-curr-connections         Number of open connections
total-connections          mc-stats-total-connections        Total number of connections opened since the server started running
connection-structures      mc-stats-connection-structures    Number of connection structures allocated by the server
cmd-get                    mc-stats-cmd-get                  Cumulative number of retrieval requests
cmd-set                    mc-stats-cmd-set                  Cumulative number of storage requests
get-hits                   mc-stats-get-hits                 Number of keys that have been requested and found present
get-misses                 mc-stats-get-misses               Number of items that have been requested and not found
evictions                  mc-stats-evictions                Number of items removed from cache because they passed their expiration time
bytes-read                 mc-stats-bytes-read               Total number of bytes read by this server from network
bytes-written              mc-stats-bytes-written            Total number of bytes sent by this server to network
limit-maxbytes             mc-stats-limit-maxbytes           Number of bytes this server is allowed to use for storage.
"
  pid uptime time version rusage-user rusage-system curr-items total-items
  bytes curr-connections total-connections connection-structures cmd-get cmd-set
  get-hits get-misses evictions bytes-read bytes-written limit-maxbytes)

;;;
;;; The main class which represents the memcached server
;;;
(defclass memcache ()
  ((name
    :initarg :name
    :reader name
    :type simple-string
    :documentation "Name of this Memcache instance")
   (ip 
    :initarg :ip
    :initform "127.0.0.1"
    :accessor ip
    :type simple-string
    :documentation "The IP address of the Memcached server this instance represents")
   (port 
    :initarg :port
    :initform 11211
    :accessor port
    :type fixnum
    :documentation "The port on which the Memcached server this instance represents runs")
   (memcached-server-storage-size 
    :initform 0
    :reader memcached-server-storage-size
    :type fixnum
    :documentation "Memory allocated to the Memcached Server")
   (pool-size
    :initarg :pool-size
    :initform 2
    :reader pool-size)
   (pool
    :reader pool))
  (:documentation "This class represents an instance of the Memcached server"))

(defmethod print-object ((mc memcache) stream)
  (format stream "#<~S ~A on ~A:~A SIZE:~AMb>"
	  (type-of mc)
	  (when (slot-boundp mc 'name) (name mc))
	  (when (slot-boundp mc 'ip) (ip mc))
	  (when (slot-boundp mc 'port) (port mc))
	  (when (slot-boundp mc 'memcached-server-storage-size) (/ (memcached-server-storage-size mc) 1024 1024))))

(defmethod initialize-instance :after ((memcache memcache) &rest initargs)
  (declare (ignore initargs))
  (setf (slot-value memcache 'pool) (make-instance 'memcache-connection-pool :name (concatenate 'simple-string (name memcache) " - Connection Pool") :max-capacity (pool-size memcache)))
  (handler-case (mc-pool-init :memcache memcache)
    (error () nil))
  (let ((stats (handler-case (mc-stats :memcache memcache)
		 (error () nil)))) 
    (if stats
	(setf (slot-value memcache 'memcached-server-storage-size) (mc-stats-limit-maxbytes stats))
	(setf (slot-value memcache 'memcached-server-storage-size) -1))))

(defun mc-make-memcache-instance (&key (ip "127.0.0.1") (port 11211) (name "Cache Interface Instance") (pool-size 2))
  "Creates an instance of class MEMCACHE which represents a memcached server"
  (make-instance 'memcache :name name :ip ip :port port :pool-size pool-size))

;;;-----------------------------------------------------------------


;;;
;;;
;;; Memcached API functionality
;;;
;;;

(defmacro mc-with-pool-y/n (&body body)
  "Macro to wrap the use-pool/dont-use-pool stuff and the cleanup
around a body of actual action statements"
  `(let (us)
     (if use-pool
	 (setf us (if *pool-get-trys?*
		      (mc-get-from-pool-with-try :memcache memcache)
		      (mc-get-from-pool :memcache memcache)))
	 (setf us (mc-make-pool-item :memcache memcache)))
     (unwind-protect
	  (when us
	    (let ((s (usocket:socket-stream us)))
	      (handler-case (progn ,@body)
		(error (c) (when use-pool
			     (mc-chuck-from-pool us memcache))
		       (error c)))))
       (if use-pool
	   (mc-put-in-pool us :memcache memcache)
	   (ignore-errors (usocket:socket-close us))))))

(defun mc-store (key data &key (memcache *memcache*) ((:command command) :set) ((:timeout timeout) 0) ((:use-pool use-pool) *use-pool*) (encoding *default-encoding*))
  "Stores data in the memcached server using the :command command.
key => key by which the data is stored. this is of type SIMPLE-STRING
data => data to be stored into the cache. data is a sequence of type (UNSIGNED-BYTE 8)
length => size of data
memcache => The instance of class memcache which represnts the memcached we want to use.
command => The storage command we want to use.  There are 3 available : set, add & replace.
timeout => The time in seconds when this data expires.  0 is never expire."
  (declare (type fixnum timeout) (type simple-string key))
  (let ((len (flex:octet-length data :external-format encoding)))
    (mc-with-pool-y/n
      (setq s (flex:make-flexi-stream s :external-format encoding))
      ;; (setq s *standard-output*)
      (write-string (string
                     (case command
                       (:set "set")
                       (:add "add")
                       (:replace "replace"))) s)
      (write-char #\Space s)
      (write-string key s)
      (write-char #\Space s)
      (write-string "0" s)
      (write-char #\Space s)
      (write-string (princ-to-string timeout) s)
      (write-char #\Space s)
      (write-string (princ-to-string len) s)
      (write-string +crlf+ s)
      (force-output s)
      (write-sequence data s)
      (write-string +crlf+ s)
      (force-output s)
      (let ((l (read-line s nil nil)))
	(remove #\Return l)))))

(defun mc-get (keys-list &key (memcache *memcache*) (use-pool *use-pool*) (encoding *default-encoding*))
  "Retrive value for key from memcached server.
keys-list => is a list of the keys, seperated by whitespace, by which data is stored in memcached
memcache => The instance of class memcache which represnts the memcached we want to use.

Returns a list of lists where each list has two elements key and value
key is of type SIMPLE-STRING
value is of type (UNSIGNED-BYTE 8)"
  (declare (type fixnum))
  (when (not (listp keys-list))
    (error "KEYS-LIST has to be of type LIST"))
  (mc-with-pool-y/n
    (setq s (flex:make-flexi-stream s :external-format encoding))
    (write-string "get " s)
    (loop for key in keys-list
       do (write-string key s)
       do (write-char #\Space s))
    (write-string +crlf+ s)
    (force-output s)
    (loop for x = (read-line s nil nil)
       until (search "END" x :test #'string-equal)
       collect (let* ((status-line (split-sequence:split-sequence #\Space x))
		      (len (parse-integer (fourth status-line)))
                      (seq (make-sequence '(vector (unsigned-byte 8)) len)))
		 (read-sequence seq s)
		 (read-line s nil nil)
		 (list (second status-line) seq)))))

(defun mc-del (key &key (memcache *memcache*) ((:time time) 0) (use-pool *use-pool*) (encoding *default-encoding*))
  "Deletes a particular 'key' and it's associated data from the memcached server"
  (declare (type fixnum time))
  (mc-with-pool-y/n
    (setq s (flex:make-flexi-stream s :external-format encoding))
    (write-string "delete " s)
    (write-string key s)
    (write-char #\Space s)
    (write-string (princ-to-string time) s)
    (write-string +crlf+ s)
    (force-output s)
    (let ((l (read-line s nil nil)))
      (remove #\Return l))))

(defun mc-incr (key &key (memcache *memcache*) (value 1) (use-pool *use-pool*) (encoding *default-encoding*))
  "Implements the INCR command.  Increments the value of a key. Please read memcached documentation for more information
key is a string
value is an integer"
  (declare (type fixnum value))
  (mc-with-pool-y/n
    (setq s (flex:make-flexi-stream s :external-format encoding))
    (write-string "incr " s)
    (write-string key s)
    (write-char #\Space s)
    (write-string (princ-to-string value) s)
    (write-string +crlf+ s)
    (force-output s)
    (let ((l (read-line s nil nil)))
      (remove #\Return l))))

(defun mc-decr (key &key (memcache *memcache*) (value 1) (use-pool *use-pool*) (encoding *default-encoding*))
  "Implements the DECR command.  Decrements the value of a key. Please read memcached documentation for more information"
  (declare (type fixnum value))
  (mc-with-pool-y/n
    (setq s (flex:make-flexi-stream s :external-format encoding))
    (write-string "decr " s)
    (write-string key s)
    (write-char #\Space s)
    (write-string (princ-to-string value) s)
    (write-string +crlf+ s)
    (force-output s)
    (let ((l (read-line s nil nil)))
      (remove #\Return l))))

(defun mc-stats-raw (&key (memcache *memcache*) (use-pool *use-pool*) (encoding *default-encoding*))
  "Returns Raw stats data from memcached server to be used by the mc-stats function"
  (mc-with-pool-y/n
    (setq s (flex:make-flexi-stream s :external-format encoding))
    (with-output-to-string (str) 
      (format s "stats~A" +crlf+)
      (force-output s)
      (loop for line = (copy-seq (read-line s))
	 do (princ line str)
	 until (search "END" line)))))

;;; High-Level Operations

(defun serialize (object &key encoding)
  (flex:string-to-octets (prin1-to-string object)
                         :external-format encoding))

(defun deserialize (sequence &key encoding)
  (let ((string (flex:octets-to-string sequence :external-format encoding)))
    (read-from-string string)))

(defun get-objects (keys-list &key (memcache *memcache*) (use-pool *use-pool*) (encoding *default-encoding*))
  (let ((items (mc-get keys-list
                       :memcache memcache
                       :use-pool use-pool
                       :encoding encoding)))
    (mapcar (lambda (item)
              (list (first item)
                    (deserialize (second item) :encoding encoding)))
            items)))

(defun mc-get-object (key-or-keys-list &key (memcache *memcache*) (use-pool *use-pool*) (encoding *default-encoding*))
  (let* ((multp (listp key-or-keys-list))
         (keys (if multp key-or-keys-list (list key-or-keys-list)))
         (items (get-objects keys
                             :memcache memcache
                             :use-pool use-pool
                             :encoding encoding)))
    (if multp
        items
        (cadar items))))

(defun mc-store-object (key object &key (memcache *memcache*) ((:command command) :set) ((:timeout timeout) 0) ((:use-pool use-pool) *use-pool*) (encoding *default-encoding*))
  (let ((data (serialize object :encoding encoding)))
    (mc-store key data
              :memcache memcache
              :command command
              :timeout timeout
              :use-pool use-pool
              :encoding encoding)))

;;;
;;; Collects statistics from the memcached server
;;;
(defun mc-stats (&key (memcache *memcache*) (use-pool *use-pool*))
  "Returns a struct of type memcache-stats which contains internal statistics from the
memcached server instance.  Please refer to documentation of memcache-stats for detailed 
information about each slot"
  (let* ((result (mc-stats-raw :memcache memcache :use-pool use-pool))
	 (l (split-sequence:split-sequence #\Return result))
	 xx temp)
    (setf temp (loop for x in l
		  do (setf xx (split-sequence:split-sequence " " x :remove-empty-subseqs t :test #'string-equal))
		  collect (list (second xx) (third xx))))
    (make-memcache-stats 
     :pid (parse-integer (second (assoc "pid" temp :test #'string-equal)) :junk-allowed t)
     :uptime (parse-integer (second (assoc "uptime" temp :test #'string-equal)) :junk-allowed t)
     :time (parse-integer (second (assoc "time" temp :test #'string-equal)) :junk-allowed t)
     :version (second (assoc "version" temp :test #'string-equal))
     :rusage-user (second (assoc "rusage_user" temp :test #'string-equal))
     :rusage-system (second (assoc "rusage_system" temp :test #'string-equal))
     :curr-items (parse-integer (second (assoc "curr_items" temp :test #'string-equal)) :junk-allowed t)
     :total-items (parse-integer (second (assoc "total_items" temp :test #'string-equal)) :junk-allowed t)
     :bytes (parse-integer (second (assoc "bytes" temp :test #'string-equal)) :junk-allowed t)
     :curr-connections (parse-integer (second (assoc "curr_connections" temp :test #'string-equal)) :junk-allowed t)
     :total-connections (parse-integer (second (assoc "total_connections" temp :test #'string-equal)) :junk-allowed t)
     :connection-structures (parse-integer (second (assoc "connection_structures" temp :test #'string-equal)) :junk-allowed t)
     :cmd-get (parse-integer (second (assoc "cmd_get" temp :test #'string-equal)) :junk-allowed t)
     :cmd-set (parse-integer (second (assoc "cmd_set" temp :test #'string-equal)) :junk-allowed t)
     :get-hits (parse-integer (second (assoc "get_hits" temp :test #'string-equal)) :junk-allowed t)
     :get-misses (parse-integer (second (assoc "get_misses" temp :test #'string-equal)) :junk-allowed t)
     :evictions (parse-integer (second (assoc "evictions" temp :test #'string-equal)) :junk-allowed t)
     :bytes-read (parse-integer (second (assoc "bytes_read" temp :test #'string-equal)) :junk-allowed t)
     :bytes-written (parse-integer (second (assoc "bytes_written" temp :test #'string-equal)) :junk-allowed t)
     :limit-maxbytes (parse-integer (second (assoc "limit_maxbytes" temp :test #'string-equal)) :junk-allowed t))))

;;; Error Conditions

(define-condition memcached-server-unreachable (error)
  ())

(define-condition memcache-pool-empty (error)
  ())

(define-condition cannot-make-pool-object (error)
  ())

(define-condition bad-pool-object (error)
  ())

;;;
;;;
;;; Memcached Pooled Access
;;;
;;;

(defclass memcache-connection-pool ()
  ((name
    :initarg :name
    :reader name
    :initform "Connection Pool"
    :type simple-string
    :documentation "Name of this pool")
   (pool
    :initform (make-queue)
    :accessor pool)
   (pool-lock
    :reader pool-lock
    :initform (make-process-lock "Memcache Connection Pool Lock"))
   (max-capacity
    :initarg :max-capacity
    :reader max-capacity
    :initform 2
    :type fixnum
    :documentation "Total capacity of the pool to hold pool objects")
   (current-size
    :accessor current-size
    :initform 0)
   (currently-in-use
    :accessor currently-in-use
    :initform 0
    :type fixnum
    :documentation "Pool objects currently in Use")
   (total-uses
    :accessor total-uses
    :initform 0
    :documentation "Total uses of the pool")
   (total-created
    :accessor total-created
    :initform 0
    :type fixnum
    :documentation "Total pool objects created")
   (pool-grow-requests
    :initform 0
    :accessor pool-grow-requests
    :type fixnum
    :documentation "Pool Grow Request pending Action")
   (pool-grow-lock
    :initform (make-process-lock "Pool Grow Lock")
    :reader pool-grow-lock))
  (:documentation "A memcached connection pool object"))

(defmethod print-object ((mcp memcache-connection-pool) stream)
  (format stream "#<~S Capacity:~d, Currently in use:~d>"
	  (type-of mcp)
	  (when (slot-boundp mcp 'max-capacity) (max-capacity mcp))
	  (when (slot-boundp mcp 'currently-in-use) (currently-in-use mcp))))

(defun mc-put-in-pool (conn &key (memcache *memcache*))
  (with-process-lock ((pool-lock (pool memcache))) 
    (enqueue (pool (pool memcache)) conn)
    (decf (currently-in-use (pool memcache)))))

(defun mc-get-from-pool (&key (memcache *memcache*))
  "Returns a pool object from pool."
  (let (pool-object (state t)) 
    (with-process-lock ((pool-lock (pool memcache)))
      (if (queue-empty-p (pool (pool memcache)))
	  (setf state nil)
	  (progn (incf (currently-in-use (pool memcache)))
		 (incf (total-uses (pool memcache)))
		 (setf pool-object (dequeue (pool (pool memcache)))))))
    (if state 
	pool-object 
	(error 'memcache-pool-empty))))

(defun mc-get-from-pool-with-try (&key (memcache *memcache*) (tries 5) (try-interval 1))
  ""
  (let ((tr 1))
    (loop
       (progn (when (> tr tries)
		(return nil))
	      (let ((conn (handler-case (mc-get-from-pool :memcache memcache)
			    (memcache-pool-empty () nil))))
		(if (not conn)
		    (progn (incf tr)
			   (warn "Memcache ~a : Connection Pool Empty! I will try again after ~d secs." (name memcache) try-interval)
			   (process-sleep try-interval))
		    (return conn)))))))

(defun mc-pool-init (&key (memcache *memcache*))
  "Cleans up the pool for this particular instance of memcache
& reinits it with POOL-SIZE number of objects required by this pool"
  (mc-pool-cleanup memcache)
  (dotimes (i (pool-size memcache))
    (mc-pool-grow-request memcache))
  (mc-pool-grow memcache))

(defun mc-make-pool-item (&key (memcache *memcache*))
  (handler-case (usocket:socket-connect (ip memcache) (port memcache) :element-type '(unsigned-byte 8))
    (usocket:socket-error () (error 'memcached-server-unreachable))
    (error () (error 'cannot-make-pool-object))))

(defun mc-pool-grow (memcache)
  (let (grow-count pool-item-list)
    (with-process-lock ((pool-grow-lock (pool memcache)))
      (setf grow-count (pool-grow-requests (pool memcache)))
      (setf pool-item-list (remove nil (loop for x from 1 to grow-count
					  collect (mc-make-pool-item :memcache memcache))))
      (loop for x from 1 to (length pool-item-list)
	 do (with-process-lock ((pool-lock (pool memcache)))
	      (enqueue (pool (pool memcache)) (pop pool-item-list))
	      (incf (total-created (pool memcache)))
	      (incf (current-size (pool memcache))))
	 do (decf (pool-grow-requests (pool memcache)))))))

(defun mc-destroy-pool-item (pool-item)
  (ignore-errors (usocket:socket-close pool-item)))

(defun mc-pool-grow-request (memcache)
  (with-process-lock ((pool-grow-lock (pool memcache)))
    (if (> (max-capacity (pool memcache)) (+ (current-size (pool memcache))
					     (pool-grow-requests (pool memcache))))
	(incf (pool-grow-requests (pool memcache)))
	(warn "CL-MC: Pool is at Capacity"))))

(defun mc-chuck-from-pool (object memcache)
  (mc-destroy-pool-item object)
  (with-process-lock ((pool-lock (pool memcache)))
    (decf (current-size (pool memcache))))
  #|(loop while (mc-pool-grow-request memcache)) 
  (mc-pool-grow memcache)|#
  (mc-pool-init :memcache memcache))

(defun mc-pool-cleanup (memcache)
  (with-process-lock ((pool-lock (pool memcache)))
    (with-process-lock ((pool-grow-lock (pool memcache)))
      (loop
	 when (queue-empty-p (pool (pool memcache)))
	 do (return)
	 else do (mc-destroy-pool-item (dequeue (pool (pool memcache)))))
      (setf (current-size (pool memcache)) 0
	    (currently-in-use (pool memcache)) 0
	    (pool-grow-requests (pool memcache)) 0
	    (total-created (pool memcache)) 0
	    (total-uses (pool memcache)) 0))))

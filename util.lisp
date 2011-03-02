(in-package #:cl-memcached)

(defconstant +crlf+
  (if (boundp '+crlf+)
      (symbol-value '+crlf+)
      (concatenate 'string
                   (string (code-char 13))
                   (string (code-char 10)))))

(defconstant +mc-END+
  (if (boundp '+mc-END+)
      (symbol-value '+mc-END+)
      (concatenate 'string
		   (string "END")
                   (string (code-char 13))
                   (string (code-char 10)))))


(defun mc-server-check (&key (memcache *memcache*))
  "Performs some basic tests on the Memcache instance and outputs a status string"
  (with-output-to-string (s)
    (let ((key "MEMCACHESERVERCHECK")
	  (data "IS THE SERVER OK ? PLEASE PLEASE PLEASE PLEASE PLEASE PLEASE PLEASE PLEASE PLEASE")
	  (server-response (mc-make-pool-item :memcache memcache)))
      (if server-response
	  (progn
	    (format s "Checking Memcached Server ~A running on ~A:~A ..." (name memcache) (ip memcache) (port memcache))
	    (format s "~%Sending data of length ~D with key ~A..." (length data) key)
	    (format s "~%Storage Command Rreturned : ~A" (handler-case (mc-store key data :memcache memcache)
							   (socket-error () (format t "~2%CANNOT CONNECT TO CACHE !~%"))
							   (error (c) (format t "GET COMMAND ERROR ~A" c))))
	    (format s "~%Trying to get back stored data with key ~A" key)
	    (format s "~%Retrieve Command Returned : ~a" (when (handler-case (mc-get (list key) :memcache memcache)
								 (socket-error () (format t "~2%CANNOT CONNECT TO CACHE !~%"))
								 (error (c) (format t "GET COMMAND ERROR ~A" c)))
							   "DATA"))
	    (format s "~%Delete Command Returned : ~A" (handler-case (mc-del key :memcache memcache)
							 (socket-error () (format t "~2%CANNOT CONNECT TO CACHE !~%"))
							 (error (c) (format t "DEL COMMAND ERROR ~A" c))))
	    (format s "~2%~a" (mc-stats :memcache memcache)))
	  (format s "~2%CANNOT CONNECT TO CACHE SERVER ! ~%")))))



(defun mc-make-benchmark-data (n)
  (with-output-to-string (s)
    (dotimes (i n)
      (write-char #\x s))))


(defun mc-benchmark (n data-size &key (memcache *memcache*) (use-pool t) (action :write))
  (let ((data (mc-make-benchmark-data data-size))) 
    (dotimes (i n)
      (let ((key (concatenate 'simple-string "key_" (princ-to-string i)))) 
	(case action 
	  (:write (mc-store key data :memcache memcache :use-pool use-pool :timeout 600))
	  (:read (mc-get (list key) :memcache memcache :use-pool use-pool)))))))



;; if you have cl-who installed, print a pretty html table for the memcached stats

#+cl-who
(defun memcached-details-table-helper (&key (memcache *memcache*) (stream *standard-output*))
  ""
  (cl-who:with-html-output-to-string (stream)
    
    (:table :border 1 :cellpadding 4 :width "90%" :style "border:solid black 4px;font-family:monospace;font-size:12px"
	    (let ((stats (cl-memcached:mc-stats :memcache memcache :use-pool nil)))
	      (cl-who:htm 
	       (:tr (:th :colspan 2 (:h4 (format stream "Name: ~A | Server IP : ~A  |  Port : ~A" (cl-memcached::name memcache) (cl-memcached::ip memcache) (cl-memcached::port memcache)))))
	       (:tr
		(:td (format stream "Process ID")) (:td (format stream "~a" (cl-memcached::mc-stats-pid stats))))
	       (:tr
		(:td (format stream "Server Uptime")) (:td (format stream "~a" (ct-utils::sec-to-human-time-str (cl-memcached::mc-stats-uptime stats)))))
	       (:tr
		(:td (format stream "System Time")) (:td (format stream "~a" (cl-memcached::mc-stats-time stats))))
	       (:tr
		(:td (format stream "Server Version")) (:td (format stream "~a" (cl-memcached::mc-stats-version stats))))
	       (:tr
		(:td (format stream "Accumulated user time")) (:td (format stream "~a" (cl-memcached::mc-stats-rusage-user stats))))
	       (:tr
		(:td (format stream "Accumulated system time")) (:td (format stream "~a" (cl-memcached::mc-stats-rusage-system stats))))
	       (:tr
		(:td (format stream "Current items stored in server")) (:td (format stream "~a" (cl-memcached::mc-stats-curr-items stats))))
	       (:tr
		(:td (format stream "Total items stored by server since starting")) (:td (:b (format stream "~a" (cl-memcached::mc-stats-total-items stats)))))
	       (:tr
		(:td (format stream "Current bytes used by server to store items")) (:td (format stream "~a Mb" (float (/ (cl-memcached::mc-stats-bytes stats) 1048576)))))
	       (:tr
		(:td (format stream "Number of open connections")) (:td (format stream "~a" (cl-memcached::mc-stats-curr-connections stats))))
	       (:tr
		(:td (format stream "Total number of connections opened since server start")) (:td (format stream "~a" (cl-memcached::mc-stats-total-connections stats))))
	       (:tr
		(:td (format stream "Number of connection structures allocated by server")) (:td (format stream "~a" (cl-memcached::mc-stats-connection-structures stats))))
	       (:tr
		(:td (format stream "Cumulative number of Retrieval requests")) (:td (:b (format stream "~a" (cl-memcached::mc-stats-cmd-get stats)))))
	       (:tr
		(:td (format stream "Cumulative number of Storage requests")) (:td (:b (format stream "~a" (cl-memcached::mc-stats-cmd-set stats)))))
	       (:tr
		(:td (format stream "Number of keys that have been requested and found present")) (:td (format stream "~a" (cl-memcached::mc-stats-get-hits stats))))
	       (:tr
		(:td (format stream "Number of items that have been requested and not found")) (:td (format stream "~a" (cl-memcached::mc-stats-get-misses stats))))
	       (:tr
		(:td (format stream "Total number of bytes read by server from network")) (:td (format stream "~a Mb" (float (/ (cl-memcached::mc-stats-bytes-read stats) 1048576)))))
	       (:tr
		(:td (format stream "Total number of bytes sent by this server to network")) (:td (format stream "~a Mb" (float (/ (cl-memcached::mc-stats-bytes-written stats) 1048576)))))
	       (:tr
		(:td (format stream "Number of bytes this server is allowed to use for storage")) (:td (format stream "~f Mb" (float (/ (cl-memcached::mc-stats-limit-maxbytes stats) 1048576))))))))))
(in-package #:cl-memcached)

;; picked these up from s-sysdeps by SVC
;; did not want to add another dependency just for this
;; 

(defun make-process-lock (name)
  "Create a named process lock object"
  #+lispworks (mp:make-lock :name name)
  #+abcl (ext:make-thread-lock)
  #+openmcl (ccl:make-lock name)
  #+allegro (mp:make-process-lock :name name)
  #+sb-thread (sb-thread:make-mutex :name name)
  #-(or lispworks abcl openmcl allegro sb-thread) 
  (declare (ignore name))
  #-(or lispworks abcl openmcl allegro sb-thread) 
  nil)

(defmacro with-process-lock ((lock) &body body)
  "Execute body wih the process lock grabbed, wait otherwise"
  ;; maybe it is safer to always use a timeout: 
  ;; `(mp:with-lock (,lock (format nil "Waiting for ~s" (lock-name ,lock)) 5) ,@body)
  ;; if the lock cannot be claimed in 5s, nil is returned: test it and throw a condition ?
  #+lispworks `(mp:with-lock (,lock) ,@body)
  #+abcl `(ext:with-thread-lock (,lock) ,@body)
  #+openmcl `(ccl:with-lock-grabbed (,lock) ,@body)
  #+allegro `(mp:with-process-lock (,lock) ,@body)
  #+sb-thread `(sb-thread:with-recursive-lock (,lock) ,@body)
  #-(or lispworks abcl openmcl allegro sb-thread) 
  (declare (ignore lock))
  #-(or lispworks abcl openmcl allegro sb-thread) 
  `(progn ,@body))

;; is missing from s-sysdeps

(defmacro process-sleep (seconds)
  "Thread sleep for seconds"
  #+allegro `(mp:process-sleep ,seconds)
  #+sb-thread `(sleep ,seconds))

;;;
;;; queue implementation from http://aima.cs.berkeley.edu/lisp/utilities/queue.lisp
;;;

(defstruct q
  (key #'identity)
  (last nil)
  (elements nil))

(defun make-empty-queue () 
  (make-q))

(defun empty-queue? (q)
  "Are there no elements in the queue?"
  (= (length (q-elements q)) 0))

(defun queue-front (q)
  "Return the element at the front of the queue."
  (elt (q-elements q) 0))

(defun remove-front (q)
  "Remove the element from the front of the queue and return it."
  (if (listp (q-elements q))
      (pop (q-elements q))
    nil))


(defun enqueue-at-end (q items)
  "Add a list of items to the end of the queue."
  ;; To make this more efficient, keep a pointer to the last cons in the queue
  (let ((items (list items))) 
    (cond ((null items) nil)
	  ((or (null (q-last q)) (null (q-elements q)))
	   (setf (q-last q) (last items)
		 (q-elements q) (nconc (q-elements q) items)))
	  (t (setf (cdr (q-last q)) items
		   (q-last q) (last items))))))

;; the wrappers

(defun make-queue ()
  ""
  #+allegro (make-instance 'mp:queue)
  #-allegro (make-empty-queue))

(defmacro enqueue (queue what)
  ""
  #+allegro `(mp:enqueue ,queue ,what)
  #-allegro `(enqueue-at-end ,queue ,what))

(defmacro dequeue (queue)
  ""
  #+allegro `(mp:dequeue ,queue)
  #-allegro `(remove-front ,queue))

(defmacro queue-empty-p (queue)
  ""
  #+allegro `(mp:queue-empty-p ,queue)
  #-allegro `(empty-queue? ,queue))

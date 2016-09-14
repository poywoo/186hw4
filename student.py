import logging

from kvstore import DBMStore, InMemoryKVStore

LOG_LEVEL = logging.WARNING

KVSTORE_CLASS = InMemoryKVStore

"""
Possible abort modes.
"""
USER = 0
DEADLOCK = 1

"""
Part I: Implementing request handling methods for the transaction handler

The transaction handler has access to the following objects:

self._lock_table: the global lock table. More information in the README.

self._acquired_locks: a list of locks acquired by the transaction. Used to
release locks when the transaction commits or aborts. This list is initially
empty.

self._desired_lock: the lock that the transaction is waiting to acquire as well
as the operation to perform. This is initialized to None.

self._xid: this transaction's ID. You may assume each transaction is assigned a
unique transaction ID.

self._store: the in-memory key-value store. You may refer to kvstore.py for
methods supported by the store.

self._undo_log: a list of undo operations to be performed when the transaction
is aborted. The undo operation is a tuple of the form (@key, @value). This list
is initially empty.

You may assume that the key/value inputs to these methods are already type-
checked and are valid.
"""
class IterQueue: 
    def __init__(self):
        self.in_stack = []
        self.out_stack = []

    def __getitem__(self, index):
        if not self.out_stack:
            while self.in_stack:
                self.out_stack.append(self.in_stack.pop())
        return self.out_stack[-index-1]

    def size(self):
        return len(self.in_stack)+len(self.out_stack)

    def empty(self):
        if len(self.in_stack)+len(self.out_stack) > 0:
            return False
        else:
            return True

    def push(self, obj):
        self.in_stack.append(obj)

    def pop(self):
        if not self.out_stack:
            while self.in_stack:
                self.out_stack.append(self.in_stack.pop())
        if len(self.out_stack) > 0:
            return self.out_stack.pop()
        else:
            return None

    def peek(self):
        if not self.out_stack:
            while self.in_stack:
                self.out_stack.append(self.in_stack.pop())
        return self.out_stack[-1]

    def contains(self, obj):
        if obj in self.in_stack: 
            return True
        elif obj in self.out_stack:
            return True
        else:
            return False

    def remove(self, obj):
        if obj in self.in_stack:
            self.in_stack = [value for value in self.in_stack if value != obj]
        if obj in self.out_stack:
            self.out_stack = [value for value in self.out_stack if value != obj]

class LockObject:
    
    def __init__(self, x=None, s=[]):
        self._X_lock = x #xid
        self._S_locks = s #list
        self._queue = IterQueue() #store (xid, lockType)

class TransactionHandler:

    def __init__(self, lock_table, xid, store):
        self._lock_table = lock_table
        self._acquired_locks = {}
        self._desired_lock = None
        self._xid = xid
        self._store = store
        self._undo_log = []

    def perform_put(self, key, value):
        """
        Handles the PUT request. You should first implement the logic for
        acquiring the exclusive lock. If the transaction can successfully
        acquire the lock associated with the key, insert the key-value pair
        into the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.
        Hint: be aware that lock upgrade may happen.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted. See the code in abort()
        for the exact format.

        @param self: the transaction handler.
        @param key, value: the key-value pair to be inserted into the store.

        @return: if the transaction successfully acquires the lock and performs
        the insertion/update, returns 'Success'. If the transaction cannot
        acquire the lock, returns None, and saves the lock that the transaction
        is waiting to acquire in self._desired_lock.
        """
        # Part 1.1: your code here!
        if not self._lock_table.has_key(key) or \
(self._lock_table.has_key(key) and (self._lock_table.get(key)._X_lock==self._xid or \
    (self._lock_table.get(key)._X_lock==None and (self._lock_table.get(key)._S_locks==[] or self._lock_table.get(key)._S_locks==[self._xid])))):
            if not self._lock_table.has_key(key):
                self._lock_table[key] = LockObject(x=self._xid)
            else: 
                if self._lock_table.get(key)._S_locks==[self._xid]: #upgrade
                    self._lock_table.get(key)._S_locks = []
                self._lock_table.get(key)._X_lock = self._xid
            self._acquired_locks[key] = 'X'
            self._undo_log += [(key, self._store.get(key))]
            self._store.put(key, value)
            return 'Success'
        else: 
            self._desired_lock = (key, 'X', value)
            self._lock_table[key]._queue.push((self._xid, 'X'))
            return None

    def perform_get(self, key):
        """
        Handles the GET request. You should first implement the logic for
        acquiring the shared lock. If the transaction can successfully acquire
        the lock associated with the key, read the value from the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.

        @param self: the transaction handler.
        @param key: the key to look up from the store.

        @return: if the transaction successfully acquires the lock and reads
        the value, returns the value. If the key does not exist, returns 'No
        such key'. If the transaction cannot acquire the lock, returns None,
        and saves the lock that the transaction is waiting to acquire in
        self._desired_lock.
        """
        # Part 1.1: your code here!
        if not self._lock_table.has_key(key) or (self._lock_table.has_key(key) and (self._lock_table.get(key)._X_lock==None or\
                                                                                   self._lock_table.get(key)._X_lock==self._xid)):
            if not self._lock_table.has_key(key):
                self._lock_table[key] = LockObject(s=[self._xid])
                self._acquired_locks[key] = 'S'
            else: 
                if self._lock_table.get(key)._X_lock==None: #if it isn't already an X lock
                    self._lock_table[key]._S_locks += [self._xid]
                    if not self._acquired_locks.has_key(key): #if it isn't already an X lock
                        self._acquired_locks[key] = 'S' 
            value = self._store.get(key)
            if value is None:
                return 'No such key'
            else:
                return value
        else: 
            self._desired_lock = (key, 'S', None)
            self._lock_table[key]._queue.push((self._xid, 'S'))
            return None

    def release_and_grant_locks(self):
        """
        Releases all locks acquired by the transaction and grants them to the
        next transactions in the queue. This is a helper method that is called
        during transaction commits or aborts. 

        Hint: you can use self._acquired_locks to get a list of locks acquired
        by the transaction.
        Hint: be aware that lock upgrade may happen.

        @param self: the transaction handler.
        """
        #remove self._xid from all queues
        for key in self._lock_table:
            self._lock_table[key]._queue.remove((self._xid, 'S'))
            self._lock_table[key]._queue.remove((self._xid, 'X'))
            
        for key in self._acquired_locks:
            #if X lock, look in queue
            lockVal = self._acquired_locks[key]
            if lockVal == 'X':
                self._lock_table[key]._X_lock = None
                if not self._lock_table[key]._queue.empty(): 
                    k = self._lock_table[key]._queue.pop() #(xid, lockType)
                    if k[1] == 'X':
                        self._lock_table[key]._X_lock = k[0]
                    if k[1] == 'S':
                        self._lock_table[key]._S_locks += [k[0]]
                        while (not self._lock_table[key]._queue.empty()) and self._lock_table[key]._queue.peek()[1] == 'S':
                            j = self._lock_table[key]._queue.pop()
                            self._lock_table[key]._S_locks += [j[0]]
            # if S lock, if no others have s lock, grant to next in queue
            if lockVal == 'S':
                if self._xid in self._lock_table[key]._S_locks:
                    self._lock_table[key]._S_locks.remove(self._xid) #remove xid from s locks
                if len(self._lock_table[key]._S_locks) == 0: 
                    if not self._lock_table[key]._queue.empty(): 
                        j = self._lock_table[key]._queue.pop() #(xid, lockType)
                        self._lock_table[key]._X_lock = j[0] #only option since it was S before
                if len(self._lock_table[key]._S_locks) == 1: #upgrade 
                    if not self._lock_table[key]._queue.empty(): 
                        if self._lock_table[key]._queue.contains((self._lock_table[key]._S_locks[0], 'X')):
                            self._lock_table[key]._queue.remove((self._lock_table[key]._S_locks[0], 'X'))
                            self._lock_table[key]._X_lock = self._lock_table[key]._S_locks[0]
                            self._lock_table[key]._S_locks = []
            pass # Part 1.2: your code here!
        self._acquired_locks = {}
        self._desired_lock = None

    def commit(self):
        """
        Commits the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.

        @return: returns 'Transaction Completed'
        """
        self.release_and_grant_locks()
        return 'Transaction Completed'

    def abort(self, mode):
        """
        Aborts the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.
        @param mode: mode can either be USER or DEADLOCK. If mode == USER, then
        it means that the abort is issued by the transaction itself (user
        abort). If mode == DEADLOCK, then it means that the transaction is
        aborted by the coordinator due to deadlock (deadlock abort).

        @return: if mode == USER, returns 'User Abort'. If mode == DEADLOCK,
        returns 'Deadlock Abort'.
        """
        while (len(self._undo_log) > 0):
            k,v = self._undo_log.pop()
            self._store.put(k, v)
        self.release_and_grant_locks()
        if (mode == USER):
            return 'User Abort'
        else:
            return 'Deadlock Abort'

    def check_lock(self):
        """
        If perform_get() or perform_put() returns None, then the transaction is
        waiting to acquire a lock. This method is called periodically to check
        if the lock has been granted due to commit or abort of other
        transactions. If so, then this method returns the string that would 
        have been returned by perform_get() or perform_put() if the method had
        not been blocked. Otherwise, this method returns None.

        As an example, suppose Joe is trying to perform 'GET a'. If Nisha has an
        exclusive lock on key 'a', then Joe's transaction is blocked, and
        perform_get() returns None. Joe's server handler starts calling
        check_lock(), which keeps returning None. While this is happening, Joe
        waits patiently for the server to return a response. Eventually, Nisha
        decides to commit his transaction, releasing his exclusive lock on 'a'.
        Now, when Joe's server handler calls check_lock(), the transaction
        checks to make sure that the lock has been acquired and returns the
        value of 'a'. The server handler then sends the value back to Joe.

        Hint: self._desired_lock contains the lock that the transaction is
        waiting to acquire.
        Hint: remember to update the self._acquired_locks list if the lock has
        been granted.
        Hint: if the transaction has been granted an exclusive lock due to lock
        upgrade, remember to clean up the self._acquired_locks list.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted.

        @param self: the transaction handler.

        @return: if the lock has been granted, then returns whatever would be
        returned by perform_get() and perform_put() when the transaction
        successfully acquired the lock. If the lock has not been granted,
        returns None.
        """
        #self._desired_lock = (key, lockType, value)
        if self._desired_lock[1] == 'X':
            if self._lock_table[self._desired_lock[0]]._X_lock == self._xid: #if X lock granted
                self._acquired_locks[self._desired_lock[0]] = 'X'
                self._undo_log += [(self._desired_lock[0], self._store.get(self._desired_lock[0]))]
                self._store.put(self._desired_lock[0], self._desired_lock[2])
                self._desired_lock = None
                return 'Success'
        elif self._desired_lock[1] == 'S': 
            if self._xid in self._lock_table[self._desired_lock[0]]._S_locks:
                self._acquired_locks[self._desired_lock[0]] = 'S'
                temp = self._desired_lock
                self._desired_lock = None
                value = self._store.get(temp[0])
                if value is None:
                    return 'No such key'
                else:
                    return value
            if self._lock_table[self._desired_lock[0]]._X_lock == self._xid: #upgraded
                self._acquired_locks[self._desired_lock[0]] = 'X'
                temp = self._desired_lock
                self._desired_lock = None
                value = self._store.get(temp[0])
                if value is None:
                    return 'No such key'
                else:
                    return value
        return None
        pass # Part 1.3: your code here!







"""
Part II: Implement deadlock detection method for the transaction coordinator

The transaction coordinator has access to the following object:

self._lock_table: see description from Part I
"""

class TransactionCoordinator:

    def __init__(self, lock_table):
        self._lock_table = lock_table

    def detect_deadlocks(self):
        """
        Constructs a waits-for graph from the lock table, and runs a cycle
        detection algorithm to determine if a transaction needs to be aborted.
        You may choose which one transaction you plan to abort, as long as your
        choice is deterministic. For example, if transactions 1 and 2 form a
        cycle, you cannot return transaction 1 sometimes and transaction 2 the
        other times.

        This method is called periodically to check if any operations of any
        two transactions conflict. If this is true, the transactions are in
        deadlock - neither can proceed. If there are multiple cycles of
        deadlocked transactions, then this method will be called multiple
        times, with each call breaking one of the cycles, until it returns None
        to indicate that there are no more cycles. Afterward, the surviving
        transactions will continue to run as normal.

        Note: in this method, you only need to find and return the xid of a
        transaction that needs to be aborted. You do not have to perform the
        actual abort.

        @param self: the transaction coordinator.

        @return: If there are no cycles in the waits-for graph, returns None.
        Otherwise, returns the xid of a transaction in a cycle.
        """
        #create graph
        graph = {}
        for key in self._lock_table:
            lockObj = self._lock_table[key]
            if lockObj._X_lock != None: 
                adjList = []
                for i in range(lockObj._queue.size()):
                    q_xid = lockObj._queue[i][0]
                    adjList += [q_xid]
                graph[lockObj._X_lock] = adjList
            if len(lockObj._S_locks) != 0:
                for i in range(len(lockObj._S_locks)):
                    adjList = []
                    for j in range(lockObj._queue.size()):
                        q_xid = lockObj._queue[j][0]
                        adjList += [q_xid]
                    graph[lockObj._S_locks[i]] = adjList    
        #dfs
        def detectCycle(graph):
            visited = set()
            if len(graph)>0:
                stack = [graph.keys()[0]]
                while stack: 
                    current = stack.pop()
                    if current in visited:
                        return current
                    else: 
                        visited.add(current)
                        neighbors = graph[current]
                        for x in neighbors:
                            stack.append(x)
                return None
        
        return detectCycle(graph)
            
        pass # Part 2.1: your code here!

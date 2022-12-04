inputOperation = [] # schedule

def simpleLocking(inputOperation):
    print("\n[??] Executing Transaction Schedule using Simple Locking\n")
    itemLockTable = {} # holding the lock on each item
    transactionTable = {} # holding active transaction
    blockedTransaction = [] # holding blocked transaction
    while len(inputOperation) > 0:
        operation = inputOperation[0]
        operation = operation.split()
        operationType = operation[0][0]
        number = operation[0][1]
        operationTask = operation[0]
        operationTask = operationTask.replace(";", "")
        if operationType == "B": # begin transaction
            transactionTable[number] = [] # add transaction to transaction table
            print("[..] [Operation : {}]".format(operationTask) + " Transaction " + number + " begins\n")
            del inputOperation[0]
        elif operationType == "C": # commit transaction
            if number in blockedTransaction: # if transaction is blocked
                # check if transaction is still blocked
                if isStillBlocked(number, itemLockTable, transactionTable, inputOperation):
                    # if transaction is still blocked, move the commit operation to the end of the schedule
                    inputOperation.append(inputOperation[0])
                    del inputOperation[0]
                else : 
                    # if transaction is not blocked, remove the transaction from the blocked transaction list
                    blockedTransaction.remove(number)
            else : # if transaction is not blocked
                if isTransactionStillHaveOperation(number, inputOperation):
                    # if transaction still have operation, move the commit operation to the end of the schedule
                    inputOperation.append(inputOperation[0])
                    del inputOperation[0]
                else :
                    print("[//] [Operation : {}]".format(operationTask) + " Transaction " + number + " commits\n")
                    for items in transactionTable[number]: # release all the locks held by the transaction
                        print("[!!] [Operation : {}]".format(operationTask) + " Transaction " + number + " releases exclusive lock on item " + items +"\n")
                        itemLockTable[items].remove(number) 
                        # if no transaction is holding the lock on the item, remove the item from the item lock table
                        if len(itemLockTable[items]) == 0: 
                            del itemLockTable[items]
                    del transactionTable[number]
                    del inputOperation[0]
                    blocked = getBlockedTransaction(transactionTable)
                    for numbers in blocked:                    
                        if not isStillBlocked(numbers, itemLockTable, transactionTable, inputOperation):
                            blockedTransaction.remove(numbers)
        elif operationType == "R": # read item
            item = operation[0][2]
            if number in blockedTransaction: # if transaction is blocked
                print("[!!] [Operation : {}]".format(operationTask) + " Transaction", number, "is blocked\n")
                inputOperation.append(inputOperation[0]) # move the operation to the end of the schedule                
                del inputOperation[0]
            elif isTransactionBlocked(number, item, itemLockTable, transactionTable, inputOperation):
                # if transaction is blocked, add current transaction to the blocked transaction list
                print("[!!] [Operation : {}]".format(operationTask) + " Transaction", number, "is blocked\n")
                print("[!!] [Operation : {}]".format(operationTask) + " Transaction", number, "is waiting for exclusive lock on item", item, "to be released\n")
                blockedTransaction.append(number)
                inputOperation.append(inputOperation[0]) # move the operation to the end of the schedule                
                del inputOperation[0]
            else:
                if isTransactionHoldingLock(number, item, itemLockTable, transactionTable, inputOperation):
                    print("[{}] [Operation : {}] Transaction".format(number, operationTask), number, "reads item", item,"\n")
                    del inputOperation[0]
                else :
                    print("[>>] [Operation : {}]".format(operationTask) + " Transaction", number, "puts exclusive lock on", item,"\n")
                    print("[{}] [Operation : {}] Transaction".format(number, operationTask), number, "reads item", item,"\n")
                    itemLockTable[item] = [number] 
                    transactionTable[number].append(item) 
                    del inputOperation[0]
        elif operationType == "W": # write item
            item = operation[0][2]
            if number in blockedTransaction: # if transaction is blocked
                print("[!!] [Operation : {}]".format(operationTask) + " Transaction", number, "is blocked\n")
                inputOperation.append(inputOperation[0]) # move the operation to the end of the schedule                
                del inputOperation[0]
            elif isTransactionBlocked(number, item, itemLockTable, transactionTable, inputOperation):
                # if transaction is blocked, add current transaction to the blocked transaction list
                print("[!!] [Operation : {}]".format(operationTask) + " Transaction", number, "is blocked\n")
                print("[!!] [Operation : {}]".format(operationTask) + " Transaction", number, "is waiting for exclusive lock on item", item, "to be released\n")
                blockedTransaction.append(number)
                inputOperation.append(inputOperation[0])  # move the operation to the end of the schedule
                del inputOperation[0]
            else:
                if isTransactionHoldingLock(number, item, itemLockTable, transactionTable, inputOperation):
                    print("[{}] [Operation : {}] Transaction".format(number, operationTask), number, "writes item", item,"\n")
                    del inputOperation[0]
                else :
                    print("[>>] [Operation : {}]".format(operationTask) + " Transaction", number, "puts exclusive lock on", item,"\n")
                    print("[{}] [Operation : {}] Transaction".format(number, operationTask), number, "writes item", item,"\n")
                    itemLockTable[item] = [number]
                    transactionTable[number].append(item)
                    del inputOperation[0]
    print("[!!] Transaction schedule is completed\n")

def isTransactionBlocked(number, item, itemLockTable, transactiontable, inputOperation):
    if item in itemLockTable: # if item is locked
        # if the transaction is not holding the lock on the item, return true
            if not isTransactionHoldingLock(number, item, itemLockTable, transactiontable, inputOperation):
                return True

def isStillBlocked(number, itemLockTable, transactiontable, inputOperation):
    # check if transaction is blocked
    for operation in inputOperation:
        operation = operation.split()
        currNumber = operation[0][1]
        currOperationType = operation[0][0]
        if currOperationType == "R" or currOperationType == "W":
            if currNumber == number:
                item = operation[0][2]
                if isTransactionBlocked(number, item, itemLockTable, transactiontable, inputOperation):
                    return True

# check if transaction already holding the lock on the item
def isTransactionHoldingLock(number, item, itemLockTable, transactiontable, inputOperation):
    if item in itemLockTable:
        if number in itemLockTable[item]:
            return True


# get all blocked transaction
def getBlockedTransaction(blockedTransaction):
    blocked = []
    for transaction in blockedTransaction:
        blocked.append(transaction)
    return blocked

# before commit, check if there is still operation for the transaction

def isTransactionStillHaveOperation(number, inputOperation):
    for operation in inputOperation:
        operation = operation.split()
        currOperationType = operation[0][0]
        currNumber = operation[0][1]
        if currOperationType == "R" or currOperationType == "W":
            if currNumber == number:
                return True

with open("transaction.txt", "r") as text:
    for line in text:
        inputOperation.append(line)

simpleLocking(inputOperation)
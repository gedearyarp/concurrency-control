inputOperation = []

def lockManager(inputOperation):
    lockTable = {}
    transactionTable = {}
    blockedTransaction = []
    while len(inputOperation) > 0:
        operation = inputOperation[0]
        operation = operation.split()
        if operation[0][0] == "B":
            transactionTable[operation[0][1]] = []
            print("Transaction " + operation[0][1] + " begins")
            del inputOperation[0]
        elif operation[0][0] == "C":
            if operation[0][1] in blockedTransaction:
                if isTransactionBlocked(operation[0][1], lockTable, transactionTable, inputOperation):
                    inputOperation.append(inputOperation[0])
                    del inputOperation[0]
                else : 
                    blockedTransaction.remove(operation[0][1])
            else : 
                for item in transactionTable[operation[0][1]]:
                    lockTable[item].remove(operation[0][1])
                    if len(lockTable[item]) == 0:
                        del lockTable[item]
                del transactionTable[operation[0][1]]
                print("Transaction " + operation[0][1] + " commits")
                del inputOperation[0]
        elif operation[0][0] == "R":
            if operation[0][2] in lockTable:
                if len(lockTable[operation[0][2]]) > 0:
                    # if operation[0][1] in blockedTransaction:
                        # inputOperation.append(inputOperation[0])
                        # del inputOperation[0]
                    # else:
                    print("Transaction", operation[0][1], "is blocked")
                    blockedTransaction.append(operation[0][1])
                    inputOperation.append(inputOperation[0])                  
                    del inputOperation[0]
            else:
                print('Transaction', operation[0][1], 'puts exclusive lock on', operation[0][2])
                print('Transaction', operation[0][1], 'reads item', operation[0][2])
                lockTable[operation[0][2]] = [operation[0][1]]
                transactionTable[operation[0][1]].append(operation[0][2])
                del inputOperation[0]
        elif operation[0][0] == "W":
            if operation[0][2] in lockTable:
                if len(lockTable[operation[0][2]]) > 0:
                    # if operation[0][1] in blockedTransaction:
                        # inputOperation.append(inputOperation[0])
                        # del inputOperation[0]
                    # else:
                    print("Transaction", operation[0][1], "is blocked")
                    blockedTransaction.append(operation[0][1])
                    inputOperation.append(inputOperation[0])  
                    del inputOperation[0]
            else:
                print('Transaction', operation[0][1], 'puts exclusive lock on', operation[0][2])
                print('Transaction', operation[0][1], 'writes item', operation[0][2])
                lockTable[operation[0][2]] = [operation[0][1]]
                transactionTable[operation[0][1]].append(operation[0][2])
                del inputOperation[0]

def isTransactionBlocked(transaction_number, locktable, transactiontable, inputOperation):
    for operation in inputOperation:
        operation = operation.split()
        if operation[0][0] == "R" or operation[0][0] == "W":
            if operation[0][2] in locktable:
                if len(locktable[operation[0][2]]) > 0:
                    return True

with open("transaction2.txt", 'r') as text:
    for line in text:
        inputOperation.append(line)

lockManager(inputOperation)
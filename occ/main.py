import threading
import time
import datetime

from transaction import Transaction
from record import Record


# Containers
TRANSACTIONS = []
RECORDS = {}
THREADS = []
LOGS = []

# Variabel global
global_initial_value = 200
global_increment = 1


def execute_transaction(transaction):

    global global_increment

    executed = False
    while not executed:

        transaction.clear_TS()
        local_records = {}

        # 1) write records to a local copy, read records either from the local copy or from the database
        transaction.set_startTS()
        for statement in transaction.get_statements():
            if statement[0] == "W":
                if statement[1] not in list(local_records.keys()):
                    local_records[statement[1]] = Record(RECORDS[statement[1]].read()[1])
                write_timestamp = local_records[statement[1]].write(global_increment)
                LOGS.append((write_timestamp, transaction.get_id(), str(str(statement[1]) + " = " + str(statement[1]) + " + " + str(global_increment)), "", "local, belum di write di database"))
                time.sleep(transaction.get_id()/100)
            else:
                if statement[1] not in list(local_records.keys()):
                    read_timestamp, execution_result = RECORDS[statement[1]].read()
                else:
                    read_timestamp, execution_result = local_records[statement[1]].read()
                LOGS.append((read_timestamp, transaction.get_id(), statement[0], statement[1], (str(statement[1]) + " = " + str(execution_result))))
                time.sleep(transaction.get_id()/100)
        
        # 2) validation on Tj succeeds if for all Ti with TS(Ti) < TS(Tj) either one of the following condition holds:
        # - finishTS(Ti) < startTS(Tj)
        # - startTS(Tj) < finishTS(Ti) < validationTS(Tj) and write set of Ti does not intersect with read set of Tj
        transaction.set_validationTS()
        LOGS.append((transaction.get_validationTS(), transaction.get_id(), "<val>", "", ""))
        validation_result = True
        for other_transaction in TRANSACTIONS:
            conditions = other_transaction.get_id() < transaction.get_id() and (other_transaction.get_finishTS() is not None) and (not((other_transaction.get_finishTS() < transaction.get_startTS()) or ((transaction.get_startTS() < other_transaction.get_finishTS()) and (other_transaction.get_finishTS() < transaction.get_validationTS()) and (other_transaction.get_write_set().isdisjoint(transaction.get_read_set())))))
            
            if conditions:
                validation_result = False
                break
        
        # 3) commit on successful validation
        if validation_result:
            for record_name in list(local_records.keys()):
                RECORDS[record_name] = local_records[record_name]
                LOGS.append((datetime.datetime.now(), transaction.get_id(), "W", str(record_name), str(str(record_name) + " = " + str(record_name) + " + " + str(global_increment))))
            transaction.set_finishTS()
            LOGS.append((transaction.get_finishTS(), transaction.get_id(), "C", "", ""))
            #time.sleep(2)
            executed = True
        else:
            LOGS.append((datetime.datetime.now(), transaction.get_id(), "A", "", "Validation fail, harus rollback"))
            transaction.clear_TS()
            local_records.clear()
            time.sleep(1)



# Main program

print("<== Implementasi Serial Optimistic Concurrency Control (OCC) ==>\n")

# Minta jumlah transaksi yang akan dieksekusi
print("Input jumlah transaksi yang ingin dijalankan")
n = int(input("Jumlah transaksi: "))

# Minta path ke file .txt masing-masing transaksi
print("\n<== Input path file .txt dari setiap transaksi yang ingin dilakukan ==>")
paths = []
for i in range(n):
    path = str(input("Path file .txt transaksi T" + str(i+1) + ": "))
    paths.append(path)

# Menginisiasi masing-masing transaksi yang akan dilakukan
print("\n<== Menginisiasi semua transaksi yang akan dilakukan ==>")
t_indexs = 1
for path in paths:
    TRANSACTIONS.append(Transaction((len(TRANSACTIONS)+1), path))
    print(f"<T{t_indexs} telah di-inisiasi>")
    t_indexs+=1
t_indexs = 0


# Generate semua record yang "diperlukan" transaksi
print("\n<== Record yang akan dibaca transaksi ==>")
print("- Nilai awal masing-masing record: " + str(global_initial_value))
print("- Nilai yang akan ditambahkan setiap kali eksekusi perintah write: " + str(global_increment))
for transaction in TRANSACTIONS:
    for record_name in transaction.get_affected_record_names():
        if record_name not in list(RECORDS.keys()):
            RECORDS[record_name] = Record(global_initial_value)
            print(str(record_name) + " = " + str(global_initial_value))
print("Selesai")

# Eksekusi 1 transaksi = 1 thread
for transaction in TRANSACTIONS:
    THREADS.append(threading.Thread(target=execute_transaction, args=(transaction,), daemon=True))

# Eksekusi semua transaksi secara bersamaan
print("\nMengeksekusi transaksi secara konkuren...")
start_time = datetime.datetime.now()
for thread in THREADS:
    thread.start()

# Tunggu sampai eksekusi semua transaksi selesai
for thread in THREADS:
    thread.join()
end_time = datetime.datetime.now()
print("Eksekusi selesai")
print("Waktu eksekusi: " + str(end_time - start_time) + " detik")
print("\nNilai akhir record:")
for record_name in list(RECORDS.keys()):
    print(str(record_name) + " = " + str(RECORDS[record_name].value))

# Hasil Eksekusi Semua Transaksi
print("\nSchedule eksekusi:")
timestamp_width = len(str(datetime.datetime.now()))
col_width = 9
for record_name in list(RECORDS.keys()):
    if ((2 * len(record_name)) + 7) > col_width:
        col_width = (2 * len(record_name)) + 7
print("Timestamp".ljust(timestamp_width) + " | ", end="")
for transaction in TRANSACTIONS:
    print(str("T" + str(transaction.get_id())).rjust(col_width) + " | ", end="")
print("Keterangan operasi Read/Write")
for log in LOGS:
    print(str(log[0]).ljust(timestamp_width) + " | ", end="")
    for _ in range(log[1] - 1):
        print("".rjust(col_width) + " | ", end="")
    if str(log[3]) != "":
        print(str(str(log[2]) + "(" + str(log[3]) + ")").rjust(col_width) + " | ", end="")
    else:
        print(str(log[2]).rjust(col_width) + " | ", end="")
    for _ in range(n - log[1]):
        print("".rjust(col_width) + " | ", end="")
    print(log[4])
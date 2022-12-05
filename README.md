# Concurrency Control Protocol

## 1. Simple Locking (Exclusive Locks Only)

Untuk menjalankan program untuk simple locking protocol , silahkan :

* Clone repository ini
* Buka terminal dan arahkan terminal hingga ke folder simple-locking
* Silahkan atur jadwal transaksi pada file transaction.txt dengan ketentuan sebelum operasi pertama suatu transaksi diberikan operasi B{nomor transaksi}
* Jalankan command `python simplelock.py` atau `python3 simplelock.py` pada terminal

## 2. Serial Optimistic Concurrency Control (OCC)

Untuk menjalankan program untuk serial optimistic concurrency control , silahkan :

* Clone repository ini
* Buka terminal dan arahkan terminal hingga ke folder occ
* Buat file txt untuk setiap transaksi yang ingin dilakukan (1 file txt untuk setiap transaksi), misalkan T1.txt, T2.txt (untuk 2 transaksi)
* Setiap file berisis statement seperti Read (R), Write (W) pada suatu record
* Jalankan command `python main.py` pada terminal
* Kemudian nanti akan diminta input dari user terkait jumlah transaksi yang ingin dilakukan dan letak file txt dari setiap transaksi

# HW3 —— Wordcount with Spark
By: 
- Shiqi Chen
- Nalongsone Danddank

This HW implements three PySpark applications using the native DataFrame API to analyze the `hamlet.txt` file.

## 1. Environment Setup

### Using WSL

```bash
wsl
lsb_release -a
```

**Output:**
```
No LSB modules are available.
Distributor ID: Ubuntu
Description:    Ubuntu 24.04.2 LTS
Release:        24.04
Codename:       noble
```

### Install Dependencies

```bash
sudo apt update
sudo apt install -y openjdk-21-jdk python3 python3-venv python3-pip
```

### Setup Python Virtual Environment

```bash
python3 -m venv ~/venv/pyspark
source ~/venv/pyspark/bin/activate
which python
python -m pip install --upgrade pip
pip install pyspark
```

## 2. File Descriptions

### WordCount.py
Counts the **total number of words** in `hamlet.txt`.

### WordFrequency.py
Finds the **top 20 most frequent words** in `hamlet.txt`, sorted in descending order.

### WordPairs.py
Finds the **top 20 most frequent pairs** in `hamlet.txt`, sorted in descending order.

## 3. Running the Programs

### WordCount.py

```bash
python3 WordCount.py
```

**Output:**
```
Total number of words: 31809
```

### WordFrequency.py

```bash
python3 WordFrequency.py
```

**Output:**
```
Top 20 most frequent words:
+----+-----+
|word|count|
+----+-----+
|the |1000 |
|of  |657  |
|to  |627  |
|and |576  |
|a   |459  |
|I   |387  |
|in  |348  |
|my  |336  |
|is  |297  |
|you |282  |
|Ham.|261  |
|his |235  |
|not |220  |
|it  |216  |
|your|211  |
|that|202  |
|with|190  |
|The |170  |
|for |155  |
|this|152  |
+----+-----+
```

### WordPairs.py

```bash
python3 WordPairs.py
```

**Output:**
```
Top 20 most frequent pairs:
(the, the) 663
(of, the) 592
(of, of) 494
(to, to) 473
(and, and) 462
(the, to) 384
(and, the) 358
(a, a) 349
(I, I) 329
(and, of) 310
(in, in) 297
(my, my) 293
(in, the) 292
(is, the) 264
(Ham., Ham.) 261
(a, of) 254
(is, is) 252
(of, to) 250
(a, the) 248
(and, to) 230
```

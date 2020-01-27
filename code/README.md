## Filter Eebo input text

**copy_eebo_files_for_input.py**

1. only gather files ending with "\_text.txt" or notes, of 50 chars or longer.
   * This should give you 1,154,995 items
   * Without filtering for note length, total comes to: 
2.





## create_blast_input.py

1. prepare data with `create_blast_input.py`

usage:
python create_blast_input.py --prefix ecco --input ../data/raw/eccotxt --output ../data/work/chunks_for_blast/ --log ../data/work/logs
python create_blast_input.py --prefix eebo --input ../data/raw/eebotxt_filtered --output ../data/work/chunks_for_blast/ --log ../data/work/logs

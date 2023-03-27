# Create input for BLAST

## copy_eebo_files_for_input.py

**Filters EEBO input text.**

1. only gather files ending with "\_text.txt" or notes, of 50 chars or longer.
   * This should give you 1,154,995 items.
   * Without filtering for note length, total comes to: 5,170,051 items.
2. outputs to data/raw/eebotxt_filtered

## create_blast_input.py

**Creates input data for BLAST.**

Each input dataset should have following fields:

`doc_id` - unique id for document
`text` - actual document text for reuse detection
`collection` - (eg.: "eebo") metadata, optional
`text_loc` - (file location) metadata, optional

### Usage
```
python create_blast_input.py --prefix ecco --input ../data/raw/eccotxt --output ../data/work/chunks_for_blast/ --log ../data/work/logs
python create_blast_input.py --prefix eebo --input ../data/raw/eebotxt_filtered --output ../data/work/chunks_for_blast/ --log ../data/work/logs
```

# Generate final results from BLAST output

## generate_json.py

**Generates results JSON from BLAST output.**

Takes 1 command line arg used to get both input and output dirs:

```
inputdir = "../data/work/" + datadir + "/"
outputdir = "../output/" + datadir + "/"
```

### Usage
`python generate_json.py test_hume5_blastdb25/data_out`

## generate_pdf.py

# Pick subset of BLAST results

## get_tr_data_output.py

```
python3 get_tr_data_output.py --idfile ../data/work/doc_ids_shakespeare.txt --outputpath ../data/work/shaketest
```

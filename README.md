# Text Reuse input data creator

Make data for BLAST text reuse detection from ecco and eebo. 

## Setup

Copy or symlink ECCO text files in `./data/raw/eccotext` and EEBO files in `./data/raw/eebotxt` maintaining the original directory structure. Eg. ECCO files would look something like this:
```
./data/raw/eccotxt/ECCO_I/ECCO_1of2/HistAndGeo/0146000100/xml/0146000100.txt
./data/raw/eccotxt/ECCO_I/ECCO_1of2/HistAndGeo/0146000500/xml/0146000500.txt
...
```

And EEBO would look like:

```
./data/raw/eebotxt/eebo_phase1/A0/A00678/A00678.headed.txt
./data/raw/eebotxt/eebo_phase1/A0/A00671/A00671.headed.txt
./data/raw/eebotxt/eebo_phase1/A0/A00671/A00671.headed_note_at_99798.txt
...
```

## Create index to ECCO and EEBO texts

Example:

`python ecco_index.py --eccodir "/home/local/vvaara/projects/comhis/data_input_text_reuse/data/raw/eccotxt/" --eebodir "/home/local/vvaara/projects/comhis/data_input_text_reuse/data/raw/eebotxt/"`


## Usage

`python create_json.py -i doc_ids_hume.txt -o hume_history_for_text_reuse.json`


# Output data generator

Turn finished text reuse data (indices in csv files) back to json with actual text indices, fragments of texts and text ids.

## Usage

`python generate_json.py --datadir DATADIR HERE --outdir OUTPUTDIR HERE --iter ITERATION NUMBER TO PROCESS`  

Whole text data needs to be in data/



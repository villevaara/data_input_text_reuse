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

## Usage

`python create_json.py -i doc_ids_hume.txt -o hume_history_for_text_reuse.json`

# Documentation

https://www.utupub.fi/bitstream/handle/10024/146706/Vesanto_Aleksi_opinnayte.pdf?sequence=1&isAllowed=y

## Running BLAST

see README in code/work/

### batches

python data_preparer.py --data_location="../../data/raw" --output_folder="../../output/blast_out" --threads=1 --language="ENG"
python blast_batches.py --batch_folder="../../output/export" --output_folder="../../output/blast_out" --threads=1 --iter=0 --text_count=5 --qpi=5

### one go

python run_full.py --data_folder="../../data/raw" --output_folder="../../output/blast_simple" --language="ENG" --threads=1

## Custom BLAST

BLAST was adjusted by A. Vesanto for the text reuse task. The version provided was 2.5.0 which unfortunately had a bug in multithreaded processing, and another version needed to be modified for processing EEBO and ECCO. The version was 2.6.0 found at http://www.repeatmasker.org/ under `RMBlast 2.6.0 BUGFIX`, with the BUGFIX patch applied. Corresponding changes were then applied to the two modifed files:

`c++/src/util/tables/sm_blosum62.c`
`c++/src/algo/blast/core/blast_stat.c`

These files are attached under `blast/` in this documentation directory.
The pre-packaged custom version of BLAST is also attached under the same directory.

# Documentation

https://www.utupub.fi/bitstream/handle/10024/146706/Vesanto_Aleksi_opinnayte.pdf?sequence=1&isAllowed=y

## Running BLAST

see README in code/work/

### batches

python data_preparer.py --data_location="../../data/raw" --output_folder="../../output/blast_out" --threads=1 --language="ENG"
python blast_batches.py --batch_folder="../../output/export" --output_folder="../../output/blast_out" --threads=1 --iter=0 --text_count=5 --qpi=5

### one go

python run_full.py --data_folder="../../data/raw" --output_folder="../../output/blast_simple" --language="ENG" --threads=1
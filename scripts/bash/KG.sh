#!/bin/bash
#SBATCH -p long              # Partition or queue. In this case, short!
#SBATCH --job-name=slurm_test    # Job name
#SBATCH --mail-type=ALL               # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=tiffany.callahan@colorado.edu
#SBATCH --nodes=1                    # Only use a single node
#SBATCH --ntasks=64                    # Run on a single CPU
#SBATCH --mem=500gb                   # Memory limit
#SBATCH --time=239:59:05               # Time limit hrs:min:sec
#SBATCH --output=/Users/tica6380/slurm_test_%j.out   # Standard output and error log
#SBATCH --error=/Users/tica6380/slurm_test_%j.err   # %j inserts job number



pwd; hostname; date

echo "You've requested $SLURM_CPUS_ON_NODE core(s)."
sleep 600
date

cd /Users/tica6380/KnowledgeGraph
date
python walking-rdf-and-owl/deepwalk_rdf/deepwalk --workers 63 --representation-size 512 --format edgelist --input KG_triples.txt --output KG_embeddings.txt --window 10 --number-walks 100 --walk-length 20
date
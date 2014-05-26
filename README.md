Ansible Playbook for NGram computation
======================================

[Ansible](http://www.ansibleworks.com/) playbook that installs a Hadoop multi-node cluster over GRNET [~okeanos](http://okeanos.grnet.gr/) I-a-a-S, downloads wikipedia dump, tokenizes it and computes N-gram frequencies. Final results are sorted by frequency

It can calculate a range of N-gram frequencies at one step to minimize running time.

Hadoop cluster setup uses [ibyron/hadoop](https://github.com/ibyron/hadoop) 

### Requirements
  - [Ansible](http://www.ansibleworks.com/) 1.5 or later (`pip install ansible`)
  - [kamaki](https://www.synnefo.org/docs/kamaki/latest/) 0.12.10 or later
 
### Execution
   ansible-playbook setup.yml -i hosts --extra-vars="my_token '<~okeanos token>' from_n=<start-n> to_n=<finish-n> threshold=<cut-off threshold"

### Parameters
Check setup.yml and imcrun.py for parameters and their default values.

### Notes
1) wikiExtractor.py is based on http://medialab.di.unipi.it/wiki/Wikipedia_Extractor version 2.5. My modifications are mostly hand-made and theres is great room for improvement.
2) parentheses, brackets, quotes, etc. are considered a whole word for N-Gram. As a result there are some pretty non-linguistic grams (e.g ") .") that are placed at the top spots.
3) Hadoop is only used for the N-gram calculations. So far all other pre- and post-processing take place on the master node, but not in Hadoop-parallel way. Master node currently has 6 cores, 8GB RAM and 100GB of storage.

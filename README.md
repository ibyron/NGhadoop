Ansible Playbook for NGram computation
======================================

[Ansible](http://www.ansibleworks.com/) playbook that installs a Hadoop multi-node cluster over GRNET [~okeanos](http://okeanos.grnet.gr/) I-a-a-S downloads wikipedia dump, tokenizes it and computes N-gram frequencies. 
It can calculate a range of N-gram frequencies at one step to minimize running time.

### Requirements
  - [Ansible](http://www.ansibleworks.com/) 1.5 or later (`pip install ansible`)
  - [kamaki](https://www.synnefo.org/docs/kamaki/latest/) 0.12.10 or later
 
### Execution
   ansible-playbook setup.yml -i hosts --extra-vars="my_token '<~okeanos token>' from_n=<starting n> to_n=<finishing n>"


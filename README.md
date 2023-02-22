# Digital Jupyter Analysis
This repository contains repeatable analysis performed against the data contained within the digital land platform. 
Jupyter will allow us to create repeatable analysis and also generate a record of analysis done before to help share lessons learned.

## Data
The repository SHOULD NOT be used to store data instead this should be downloaded froom our service when notebooks are ran. the data directory is ignored by github and can be used as local storage to sync data to. BE WARNED that many files we work with can be extremely large so be careful when running others analysis.

## Structure
each peice of analysis should be stored within a separate folder. We should avoid editing other's analysis accept to update when it's no longer working. creating additional jupyter notebooks within the folder can be a way of reusing general analysis.

## Dependencies
We aim to set up the same environment as in the pipline, hence primarily installing digital-land-python but there may be additional tools for data manipulation or visualisatino that can be added to the requirements. You can also change requirements files to download specific branches of digital-land-python to test new features before merging.

## Workflow
as analysis is added ensure to update this README below with a brief discription of what it aims to achieve as well an entry notebook for users to open.





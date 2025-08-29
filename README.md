# Description and goal
This project aims to create a script that can syncronize data between the public database GeoFA (GFA) and the local database in Vejle Kommune (VK) which will be the master database.

The challenge is, that the local database has contains all/most the columns of GFA, while VK contains added columns. 
Furthermore, the object ID must be generated on GFA in order to be able to syncronize. 

The script must be able to:
    - Identify new objects in VK, ask GFA to generate an object on GFA. The GFA data on the new object will be populated into VK, including GFA Object ID etc. The data on the new object in VK must be updated on GFA, which is now possible since they share the reference of object ID.
    - Identify objects on VK that has been updated/changed since last run.
    - Syncronize relevant columns from VK to GFA on the rows that has been changed.

Additionally, the script must include logging for the staff to verify if the sync was successful or not, and provide valuable error messages to assist in debugging.



# To do
## Test Env
    - download test data from VK and GFA
    - Identify the ideal data on VK and GFA after sync

### VK 
    - Get DB schema
    - Model a mock database from the schema with the data
    - Populate it with test data
    - Ability to reset (re-instantiate DB with unaffected test data) 
    - Unit tests to verify that the data is as expected after syncronization
    
### GFA
    - Get DB schema
    - Model a mock database from the schema with the data
    - Populate it with test data
    - Ability to reset (re-instantiate DB with unaffected test data) 
    - Unit tests to verify that the data is as expected after syncronization

### Sync script test (recursively)
    - With the code that Bijan has provided develop a script that can interact with mock databases
    - In collaboration with staff (Brian and Henriette), identify issues with syncronization 
    - Create functions & general rules that transform the data towards a more ideal dataset

### Logging
    - Develop logging script to generate logs on runtime behaviour (success, fail, error messages)
    - Develop logs for runtime lookups, so in the event of unsuccessful sync, the script will check data edited up until last successful run date/time
    - Develop logs for what data has been handled, from what to what. This is important for debugging, but may need to develop it so it only keeps logs from the previous 5 runs or 1 month etc due to GDPR.


## Deployment
    Once the test script reaches an acceptable data transformation:
    - Devlop script to interact with GFA API (see Bijans code)
    - Develop script to interact with VK
    - Run script on live data and verify outcome
    - Setup script run schedule on GISBATCH server 
